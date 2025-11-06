/*#include"../duckdb_lib/duckdb.h"*/
#include<stdlib.h>
#include<stdio.h>

#include "duckdb.h"
#include <stdio.h>
#include <stdlib.h>

// ------------------------------------------------------
// The divisorsif UDF logic
// ------------------------------------------------------
static void divisorsif_function(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {

	fprintf(stderr, "INSIDE DIVISORSIF UDF\n");

	// Get vector of integers from input chunk. DuckDB processes things in a vectorized fashion.
	duckdb_vector column = duckdb_data_chunk_get_vector(input, 0); 	
	int32_t *column_data = (int32_t *) duckdb_vector_get_data(column);

	idx_t rowsPerVector = duckdb_data_chunk_get_size(input);
	
	// Inferring from the docs (saw it in the vectors section), validity probably
	// just checks whether that element is NULL. duckdb_vector_get_validity will
	// presumably just return an array of ints that will tell us if a certain
	// row of this vector is non-null.
	uint64_t *col_validity = duckdb_vector_get_validity(column);

	// the output should be a vector of lists. The actual data for all lists is 
	// stored elsewhere in a contiguous space. Each element in a vector of lists merely
	// tells us the offset and length in that contiguous space that belongs to the
	// list we want. Goal is to first figure out the number of elements, then 
	// use duckdb_list_reserve to set the size, and copy all relevant data into the
	// list.
	
	/*int32_t *temp_list = (int32_t *)malloc(sizeEstimate*sizeof(int32_t));*/
	/*int32_t *arrTempLists[2];*/
	/*arrTempLists[0] = temp_list;*/
	/*int switches = 0;*/
		
	int elementsSoFar = 0;

	duckdb_list_entry *entries = (duckdb_list_entry *)duckdb_vector_get_data(output);
	uint64_t *entry_validity = duckdb_vector_get_validity(output);

	int sizeEstimate = 1024;
	duckdb_list_vector_reserve(output, sizeEstimate);

	duckdb_vector child_vec = duckdb_list_vector_get_child(output);
	int32_t *child_data = (int32_t *)duckdb_vector_get_data(child_vec);

	fprintf(stderr, "About to iterate through rows\n");
	for (idx_t row = 0; row < rowsPerVector; row++) {
        if (duckdb_validity_row_is_valid(col_validity, row)) {

			fprintf(stderr, "Row is valid, finding divisors\n");
			// now we come to the actual computation:
			// creating a list of divisors of column_data[row]
			// the idea is to find out how many divisors each number has and
			// create an (offset, length) struct for them
			int32_t n = column_data[row];
			int32_t offset = elementsSoFar;
			int32_t sizeOfThisList = 0;
			for(int32_t i = 1; i < n; i++)
			{
				if(n % i == 0)
				{
					/*temp_list[elementsSoFar] = i; */
					child_data[elementsSoFar] = i;
					elementsSoFar++;
					sizeOfThisList++;

					if(elementsSoFar == sizeEstimate) {
						sizeEstimate *= 2;
						duckdb_list_vector_reserve(output, sizeEstimate); // final capacity
						child_vec = duckdb_list_vector_get_child(output);
						child_data = (int32_t *)duckdb_vector_get_data(child_vec);
					}

				}
			}

			entries[row].offset = offset;
			entries[row].length = sizeOfThisList;

        } else {
            printf("Row %ld is invalid \n", row);
        }
        
    }

	duckdb_list_vector_set_size(output, elementsSoFar+1);
		
}

static void register_divisorsif(duckdb_connection conn) {

	printf("Registering UDF...\n");
	/*divisors_if_init(conn);*/
	duckdb_scalar_function divisorsIf = duckdb_create_scalar_function();
	duckdb_scalar_function_set_name(divisorsIf, "divisorsIf");

	duckdb_logical_type int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	duckdb_scalar_function_add_parameter(divisorsIf, int_type);
	duckdb_destroy_logical_type(&int_type);
	/*duckdb_scalar_function_add_parameter(divisorsIf, DUCKDB_TYPE_INTEGER);*/

	// return type: create a list of integers
	duckdb_logical_type subtype = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	/*duckdb_list_type_set_child_type(list_type, subtype);*/
	duckdb_logical_type list_type = duckdb_create_list_type(subtype);
	/*duckdb_destroy_logical_type(&subtype);*/
	duckdb_scalar_function_set_return_type(divisorsIf, list_type);
	duckdb_destroy_logical_type(&list_type);


	// Attach implementation
	duckdb_scalar_function_set_function(divisorsIf, divisorsif_function);

	// Register the function
	if (duckdb_register_scalar_function(conn, divisorsIf) != DuckDBSuccess) {
		fprintf(stderr, "Failed to register divisorsif UDF.\n");
	}

	duckdb_destroy_scalar_function(&divisorsIf);
}


// ------------------------------------------------------
// Test harness
// ------------------------------------------------------
int main() {
    printf("Initializing DuckDB...\n");
    duckdb_database db;
    duckdb_connection conn;
    if (duckdb_open(NULL, &db) != DuckDBSuccess) {
        fprintf(stderr, "Failed to open DuckDB.\n");
        return 1;
    }
    if (duckdb_connect(db, &conn) != DuckDBSuccess) {
        fprintf(stderr, "Failed to connect.\n");
        duckdb_close(&db);
        return 1;
    }

	printf("Registering divisorsif()...\n");
    register_divisorsif(conn);


    printf("Executing query...\n");
    duckdb_result result;
    if (duckdb_query(conn, "SELECT i, divisorsIf(i::INTEGER) AS divs FROM range(1, 10) tbl(i);", &result) != DuckDBSuccess) {
        fprintf(stderr, "Query failed: %s\n", duckdb_result_error(&result));
        duckdb_disconnect(&conn);
        duckdb_close(&db);
        return 1;
    }

	while(true) {
		duckdb_data_chunk res = duckdb_fetch_chunk(result);
		if (!res) {
			// result is exhausted
			break;
		}

		idx_t row_count = duckdb_data_chunk_get_size(res);
		// get the list column
		duckdb_vector list_col = duckdb_data_chunk_get_vector(res, 1);
		duckdb_list_entry *list_data = (duckdb_list_entry *) duckdb_vector_get_data(list_col);
		uint64_t *list_validity = duckdb_vector_get_validity(list_col);
		// get the child column of the list
		duckdb_vector list_child = duckdb_list_vector_get_child(list_col);
		int32_t *child_data = (int32_t *) duckdb_vector_get_data(list_child);
		uint64_t *child_validity = duckdb_vector_get_validity(list_child);

		// iterate over the rows
		for (idx_t row = 0; row < row_count; row++) {
			if (!duckdb_validity_row_is_valid(list_validity, row)) {
				// entire list is NULL
				printf("NULL\n");
				continue;
			}
			// read the list offsets for this row
			duckdb_list_entry list = list_data[row];
			printf("[");
			for (idx_t child_idx = list.offset; child_idx < list.offset + list.length; child_idx++) {
				if (child_idx > list.offset) {
					printf(", ");
				}
				if (!duckdb_validity_row_is_valid(child_validity, child_idx)) {
					// col1 is NULL
					printf("NULL");
				} else {
					printf("%d", child_data[child_idx]);
				}
			}
			printf("]\n");
		}

		duckdb_destroy_data_chunk(&res);

	}
	
    duckdb_destroy_result(&result);
    duckdb_disconnect(&conn);
    duckdb_close(&db);
    return 0;
}




