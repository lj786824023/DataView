SELECT
	owner,
	object_type,
	object_name
FROM
	all_procedures
WHERE
	object_tyPe IN ( 'FUNCTION', 'PROCEDURE' )