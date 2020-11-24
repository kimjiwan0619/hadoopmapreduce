import MapReduce
import sys

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # TODO: implement this class
    value = record
    if record[0] == 'a':
        for k in range(5):
            key = (record[1], k)
            mr.emit_intermediate(key, value)
    if record[0] == 'b':
        for k in range(5):
            key = (k,record[2])
            mr.emit_intermediate(key, value)
def reducer(key, list_of_values):
    # TODO: implement this class
    a = [0]*5
    b = [0]*5
    #print(list_of_values)	
    for element in list_of_values:
        if element[0] == 'a':       
            a[element[2]] = element[3]
        if element[0] == 'b':
            b[element[1]] = element[3]
    res = 0
    outrow=list(key)
    for k in range(5):
        res = res + a[k] * b[k]
    outrow.append(res)
    mr.emit((outrow))
# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
