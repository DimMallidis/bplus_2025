#ifndef RECORD_GENERATOR_H
#define RECORD_GENERATOR_H

#include "record.h"

/**
 * @brief Returns the schema for the employee table.
 * @return TableSchema for employee.
 */
TableSchema employee_get_schema();

/**
 * @brief Returns the schema for the student table.
 * @return TableSchema for student.
 */
TableSchema student_get_schema();

/**
 * @brief Generates a random employee record.
 * @param schema Pointer to the table schema.
 * @param record Pointer to the record to populate.
 */
void employee_random_record(const TableSchema *schema, Record *record);

/**
 * @brief Generates a random student record.
 * @param schema Pointer to the table schema.
 * @param record Pointer to the record to populate.
 */
void student_random_record(const TableSchema *schema, Record *record);

#endif // RECORD_GENERATOR_H
