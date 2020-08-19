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

void _parse_dr(char* filename, vector<Result>* results);

#endif
