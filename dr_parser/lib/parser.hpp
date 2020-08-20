#ifndef PARSER_H
#define PARSER_H

#include <string>
#include <vector>

using namespace std;

struct Row {
  float HMJD;
  float mag;
  float magerr;
  float clrcoeff;
  int catflags;
};

struct Header {
  string ObjectID;
  int nEpochs;
  int FilterID;
  int FieldID;
  int RcID;
  float ObjRA;
  float ObjDec;
};

struct Result {
  string ObjectID;
  int nEpochs;
  int FilterID;
  int FieldID;
  int RcID;
  float ObjRA;
  float ObjDec;
  float HMJD;
  float mag;
  float magerr;
  float clrcoeff;
  int catflags;
};

class CppDRParser{
  int nObjects;
  int batchSize;
  vector<Header> objects;
  char* filename;
  FILE *fp;

  public:
    CppDRParser();
    CppDRParser(char* file_name, int batch_size);
    ~CppDRParser();
    vector<Header> getObjects();
    void parse_dr(vector<Result>* results);
};

#endif
