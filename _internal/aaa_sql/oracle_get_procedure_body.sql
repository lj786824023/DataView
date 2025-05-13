SELECT
	listagg ( text ) WITHIN GROUP ( ORDER BY line )
FROM
	all_source
WHERE
	owner = :DATABASE_NAME
	AND name = :PROCEDURE_NAME