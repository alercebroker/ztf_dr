#include "parser.hpp"
#include <string>
#include <sstream>
#include <vector>
#include <cstring>
#include <iostream>

using namespace std;

void parse_header(string line, struct Header *header) {
  istringstream iss(line);
  vector<string> result;
  for(string s; iss >> s; )
      result.push_back(s);

  header->ObjectID = result.at(0);
  header->nEpochs = stoi(result.at(1));
  header->FilterID = stoi(result.at(2));
  header->FieldID = stoi(result.at(3));
  header->RcID = stoi(result.at(4));
  header->ObjRA = stof(result.at(5));
  header->ObjDec = stof(result.at(6));
}


void parse_row(string line, struct Row *row) {
  istringstream iss(line);
  vector<string> result;
  for(string s; iss >> s; )
      result.push_back(s);
  row->HMJD = stof(result.at(0));
  row->mag = stof(result.at(1));
  row->magerr = stof(result.at(2));
  row->clrcoeff = stof(result.at(3));
  row->catflags = stof(result.at(4));
}

void mix_data(struct Row row, struct Header header, struct Result *result){
  result->ObjectID = header.ObjectID;
  result->nEpochs = header.nEpochs;
  result->FilterID = header.FilterID;
  result->FieldID = header.FieldID;
  result->RcID = header.RcID;
  result->ObjRA = header.ObjRA;
  result->ObjDec = header.ObjDec;
  result->HMJD = row.HMJD;
  result->mag = row.mag;
  result->magerr = row.magerr;
  result->clrcoeff = row.clrcoeff;
  result->catflags = row.catflags;
}

void _parse_dr(char* filename, vector<Result>* results){
    FILE *fp;
    char * line = NULL;
    ssize_t read;
    size_t len = 0;
    struct Header objectMeta;
    struct Row objectData;
    struct Result objectResult;

    fp = fopen(filename, "r");

    if (fp == NULL)
   {
      exit(EXIT_FAILURE);
   }

    while ((read = getline(&line, &len, fp)) != -1) {
      if(line[0] == '#'){
        memmove(line, line+1, strlen(line));
        parse_header(line, &objectMeta);
      }else{
        parse_row(line, &objectData);
        mix_data(objectData, objectMeta, &objectResult);
        (*results).push_back(objectResult);
      }
    }
    fclose(fp);

}
