DELIMITER |

CREATE DEFINER="gbase"@"%" PROCEDURE "pr_create_pddl"(
	in	in_tab		varchar(100)
)
begin
/*
 * create by ljz on 2021/10/22 11:13
 * in_tab 表中文或英文名，模糊匹配
 */
	select
		t_tab_eng_name,
		t_tab_desc,
		etl_algorithm,
		'drop table if exists pdm.'
		||t_tab_eng_name
		||';\n'
		||'create table pdm.'
		||t_tab_eng_name
		||'\n(\n'
		||column_context
		||'\n)'
		||case
			when t_tab_eng_name not like 't99%'
			then '\ndistributed by ('''||replace(logical_pri_key,',',''',''')||''')\n'
			else 'replicated\n'
		end
		||'compress(5, 5)\ndefault charset=utf8\n'
		||'comment='''
		||t_tab_desc
		||''';'
		as pddl
	from (
		select
			t1.t_tab_eng_name
			,t1.logical_pri_key
			,t1.etl_algorithm
			,t1.t_tab_desc
			,group_concat
			(
				'  '
				||t2.col_eng_name
				||' '
				||t2.col_type_desc
				||' not null comment '''
				||t2.col_chn_name
				||''''
				||' '||case when lower(t2.col_type_desc) like 'varchar%' then 'default '''' ' else '' end
				order by t2.col_seq asc
				separator ',\n'
			) as column_context
		from etl.datamapping_task t1
		inner join etl.etl_pddl t2
			on t1.t_tab_eng_name = t2.tab_eng_name
		where lower(t1.t_tab_eng_name) like '%'||lower(in_tab)||'%' or lower(t1.t_tab_desc) like '%'||lower(in_tab)||'%'
		group by 1,2,3,4
	) c1;

end |