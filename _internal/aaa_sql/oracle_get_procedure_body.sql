SELECT
	listagg ( text ) WITHIN GROUP ( ORDER BY line )
FROM
	all_source
WHERE
	owner = UPPER(:DATABASE_NAME)
	AND name = UPPER(:PROCEDURE_NAME)