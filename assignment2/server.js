import express from 'express';
import { Client } from '@elastic/elasticsearch';
import cors from 'cors';
import fs from 'fs';
import csv from 'csv-parser';

const app = express();
app.use(cors());
const port = 3000;

const esClient = new Client({
  node: 'http://localhost:9200',
  auth: {
    username: 'elastic',
    password: 'WjjIoMGu*EhZfysG_CQg',
  },
});

// FUNCTION DEFINITION
async function deleteCollection(collectionName) {
  try {
    const indexExists = await esClient.indices.exists({ index: collectionName });
    if (indexExists.body) {
      await esClient.indices.delete({ index: collectionName });
      console.log(`Collection ${collectionName} deleted.`);
    }
  } catch (error) {
    console.error("Error deleting collection:", error);
    throw new Error('Error deleting collection');
  }
}

async function createCollection(collectionName) {
  try {
    const indexExists = await esClient.indices.exists({ index: collectionName });
    if (!indexExists.body) {
      await esClient.indices.create({
        index: collectionName,
        body: {
          mappings: {
            properties: {
              employee_id: { type: 'keyword' },
              name: { type: 'text' },
              age: { type: 'integer' },
              position: { type: 'text' },
              salary: { type: 'float' },
              hire_date: { type: 'date' },
              department: { type: 'text' },
              gender: { type: 'text' },
            },
          },
        },
      });
      console.log(`Collection ${collectionName} created.`);
    } else {
      console.log(`Collection ${collectionName} already exists.`);
    }
  } catch (error) {
    console.error("Error creating collection:", error);
    throw new Error('Error creating collection');
  }
}

async function indexData(collectionName, excludeColumn) {
  const employeeData = [];
  fs.createReadStream('employee_sample_data.csv')
    .pipe(csv())
    .on('data', (row) => {
      const { [excludeColumn]: excluded, ...employee } = row;
      employeeData.push({ index: { _index: collectionName } });
      employeeData.push(employee);
    })
    .on('end', async () => {
      try {
        const response = await esClient.bulk({
          refresh: true,
          body: employeeData,
        });

        if (response.errors) {
          console.error('Errors occurred during bulk indexing:', response.errors);
        } else {
          console.log('Data indexed successfully');
        }
      } catch (error) {
        console.error('Error indexing employee data:', error);
      }
    });
}

async function searchByColumn(collectionName, columnName, columnValue) {
  try {
    const response = await esClient.search({
      index: collectionName,
      body: {
        query: {
          match: {
            [columnName]: columnValue,
          },
        },
      },
    });
    return response.body.hits.hits;
  } catch (error) {
    console.error('Error searching by column:', error);
    throw new Error('Error searching by column');
  }
}

async function getEmpCount(collectionName) {
  try {
    const response = await esClient.count({
      index: collectionName,
    });
    return response.body.count !== undefined ? response.body.count : 0;
  } catch (error) {
    console.error('Error getting employee count:', error);
    throw new Error('Error getting employee count');
  }
}

async function delEmpById(collectionName, employeeId) {
  try {
    const response = await esClient.deleteByQuery({
      index: collectionName,
      body: {
        query: {
          match: { employee_id: employeeId },
        },
      },
    });
    return response.body.deleted;
  } catch (error) {
    console.error('Error deleting employee by ID:', error);
    throw new Error('Error deleting employee by ID');
  }
}

async function getDepFacet(collectionName) {
  try {
    const response = await esClient.search({
      index: collectionName,
      body: {
        aggs: {
          departments: {
            terms: {
              field: 'department.keyword',
            },
          },
        },
        size: 0,
      },
    });
    return response.body.aggregations && response.body.aggregations.departments ? response.body.aggregations.departments.buckets : [];
  } catch (error) {
    console.error('Error getting department facet:', error);
    throw new Error('Error getting department facet');
  }
}

// FUNCTION EXECUTION
const v_nameCollection = 'evil';
const v_phoneCollection = '3333';

app.get('/execute-tasks', async (req, res) => {
  try {
    await deleteCollection(v_nameCollection);
    await deleteCollection(v_phoneCollection);

    await createCollection(v_nameCollection);
    await createCollection(v_phoneCollection);

    const empCountNameCollection = await getEmpCount(v_nameCollection);
    console.log(`Employee count in ${v_nameCollection}: ${empCountNameCollection}`);

    await indexData(v_nameCollection, 'Department');
    await indexData(v_phoneCollection, 'Gender');

    const deletedCount = await delEmpById(v_nameCollection, 'E02003');
    console.log(`Deleted employee count: ${deletedCount}`);

    const empCountAfterDelete = await getEmpCount(v_nameCollection);
    console.log(`Employee count after deletion in ${v_nameCollection}: ${empCountAfterDelete}`);

    const itEmployees = await searchByColumn(v_nameCollection, 'Department', 'IT');
    console.log(`IT Employees:`, itEmployees);

    const maleEmployees = await searchByColumn(v_nameCollection, 'Gender', 'Male');
    console.log(`Male Employees:`, maleEmployees);

    const phoneDeptEmployees = await searchByColumn(v_phoneCollection, 'Department', 'IT');
    console.log(`Phone collection IT Employees:`, phoneDeptEmployees);

    const depFacetName = await getDepFacet(v_nameCollection);
    console.log(`Department facet for ${v_nameCollection}:`, depFacetName);

    const depFacetPhone = await getDepFacet(v_phoneCollection);
    console.log(`Department facet for ${v_phoneCollection}:`, depFacetPhone);

    res.send('Tasks executed successfully. Check console for details.');
  } catch (error) {
    console.error('Error executing tasks:', error);
    res.status(500).send('Error executing tasks');
  }
});

app.listen(port, () => {
  console.log('Server running on port', port);
});
