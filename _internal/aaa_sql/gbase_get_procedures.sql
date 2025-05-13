SELECT
	trim(routine_schema) as routine_schema,
	routine_type,
	routine_name
FROM
	information_schema.ROUTINES