def shiftRight(arr, d)
  n = arr.length
  d = d % n
  temp = arr[-d..-1]
  (n - d - 1).downto(0) do |i|
  arr[i + d] = arr[i]
  end
  (0...d).each do |i|
  arr[i] = temp[i]
  end
  arr
 end