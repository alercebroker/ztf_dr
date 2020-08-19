# distutils: language = c++
import cython
from libcpp.vector cimport vector
from libcpp.string cimport string

cdef extern from "parser.hpp":
  struct Result:
    string ObjectID
    int nEpochs
    int FilterID
    int FieldID
    int RcID
    float ObjRA
    float ObjDec
    float HMJD
    float mag
    float magerr
    float clrcoeff
    int catflags

  void _parse_dr(char* filename, )


def parse(filename):
  cdef vector[Result] results
  filename = filename.encode('utf-8')
  _parse_dr(filename, &results)
  return results
