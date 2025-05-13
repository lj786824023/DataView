SELECT
	table_name,
	table_comment,
	ordinal_position,
	column_name,
	column_type,
	column_comment
FROM
	(
	SELECT
		table_name,
		table_comment
	FROM
		information_schema.TABLES
	WHERE
		table_schema = %(DATABASE_NAME)s
		AND table_name = %(TABLE_NAME)s
	) t
	LEFT JOIN (
	SELECT
		ordinal_position,
		column_name,
		column_type,
		column_comment
	FROM
		information_schema.COLUMNS
	WHERE
		table_schema = %(DATABASE_NAME)s
		AND table_name = %(TABLE_NAME)s
	) t1 ON 1 = 1
ORDER BY
	ordinal_position