SELECT
	routine_definition 
FROM
	information_schema.ROUTINES 
WHERE
	routine_schema = %(DATABASE_NAME)s
	AND routine_name = %(PROCEDURE_NAME)s