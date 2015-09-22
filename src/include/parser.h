//
// Created by Siv Lu on 9/20/15.
//

#ifndef BLUS_CS165_2015_BASE_PARSER_H
#define BLUS_CS165_2015_BASE_PARSER_H

#include "data_structure.h"

/*
 * Takes a query string and parses it into the query_org struct
 */
void parse_query(char* query, query_org** res);

/*
 * Takes a query_org struct, parse it into a db_operator
 */
void parse_query_org(query_org* query, db_operator** op);
#endif //BLUS_CS165_2015_BASE_PARSER_H
