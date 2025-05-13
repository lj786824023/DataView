DELIMITER |

CREATE DEFINER="cq_sjpt"@"%" PROCEDURE "pr_datamapping_contrast"(out OUT_RES_MSG VARCHAR(200))
lable:begin
/*
 * remark,每日5点对比comm.datamapping 和生产etl.datamapping ，记录差异到chk.datamapping_contrast表；（datamapping_task相同 ）
 * 20220328 新建whd
 *  
 * 
 * 返回值：OUT_RES_MSG
 *  0	成功
 *  12	失败
 * 
 * */
	
	declare exit handler for sqlexception
	begin 
		 GET DIAGNOSTICS condition 1 OUT_RES_MSG = message_text; 
		 -- set OUT_RES_MSG ='12';
	end; 
	
	/********************************* datamapping 表差异记录**********************************/
	
	drop temporary table IF exists   etl.vt_bak_datamapping;
	create temporary table etl.vt_bak_datamapping like etl.datamapping;
	
-- 插入当日备份数据到临时表 
	insert into etl.vt_bak_datamapping 
		select seq_num,
			t_tab_eng_name,
			t_tab_chn_name,
			t_col_eng_name,
			t_col_chn_name,
			t_col_datatype,
			t_col_desc,
			t_his_flag,
			t_desc,
			s_group_id,
			s_system,
			s_tab_eng_name,
			s_tab_chn_name,
			s_col_eng_name,
			s_col_chn_name,
			s_col_datatype,
			s_tab_pk,
			s_col_desc,
			s_col_code_desc,
			e_priority,
			e_trans_rule,
			e_desc,
			r_trans_exp,
			r_val_rule,
			r_is_join,
			r_join_source_tabname,
			r_join_market_tabname,
			r_join_type,
			r_join_condition,
			r_where_condition,
			r_exec_vt,
			r_desc,
			updatetime from comm.bak_datamapping where cast(etl_date as date) = curdate();
			
		-- 支持重跑	
		 delete from etl.datamapping_contrast where cast(etl_date as date) = curdate();
		 
		 -- 备份表minus生产表
		 insert into etl.datamapping_contrast
		  select seq_num, t_tab_eng_name, t_tab_chn_name, t_col_eng_name, t_col_chn_name, t_col_datatype, t_col_desc, t_his_flag, t_desc, s_group_id, s_system, s_tab_eng_name, s_tab_chn_name, s_col_eng_name, s_col_chn_name, s_col_datatype, 
				s_tab_pk, s_col_desc, s_col_code_desc, e_priority, e_trans_rule, e_desc, r_trans_exp, r_val_rule, r_is_join, r_join_source_tabname, r_join_market_tabname, r_join_type, r_join_condition, r_where_condition, r_exec_vt, 
				r_desc, updatetime, curdate() 
		  from etl.vt_bak_datamapping 
		  MINUS
		  select seq_num, t_tab_eng_name, t_tab_chn_name, t_col_eng_name, t_col_chn_name, t_col_datatype, t_col_desc, t_his_flag, t_desc, s_group_id, s_system, s_tab_eng_name, s_tab_chn_name, s_col_eng_name, s_col_chn_name, s_col_datatype, 
				s_tab_pk, s_col_desc, s_col_code_desc, e_priority, e_trans_rule, e_desc, r_trans_exp, r_val_rule, r_is_join, r_join_source_tabname, r_join_market_tabname, r_join_type, r_join_condition, r_where_condition, r_exec_vt, 
				r_desc, updatetime, curdate() 
		  from etl.datamapping; 
		  
		  -- 生产表minus备份表
		  insert into etl.datamapping_contrast
		  select seq_num, t_tab_eng_name, t_tab_chn_name, t_col_eng_name, t_col_chn_name, t_col_datatype, t_col_desc, t_his_flag, t_desc, s_group_id, s_system, s_tab_eng_name, s_tab_chn_name, s_col_eng_name, s_col_chn_name, s_col_datatype, 
				s_tab_pk, s_col_desc, s_col_code_desc, e_priority, e_trans_rule, e_desc, r_trans_exp, r_val_rule, r_is_join, r_join_source_tabname, r_join_market_tabname, r_join_type, r_join_condition, r_where_condition, r_exec_vt, 
				r_desc, updatetime, curdate() 
		  from etl.datamapping 
		  MINUS
		  select seq_num, t_tab_eng_name, t_tab_chn_name, t_col_eng_name, t_col_chn_name, t_col_datatype, t_col_desc, t_his_flag, t_desc, s_group_id, s_system, s_tab_eng_name, s_tab_chn_name, s_col_eng_name, s_col_chn_name, s_col_datatype, 
				s_tab_pk, s_col_desc, s_col_code_desc, e_priority, e_trans_rule, e_desc, r_trans_exp, r_val_rule, r_is_join, r_join_source_tabname, r_join_market_tabname, r_join_type, r_join_condition, r_where_condition, r_exec_vt, 
				r_desc, updatetime, curdate()  
		  from etl.vt_bak_datamapping;
		  
		  
	/********************************* datamapping_task 表差异记录**********************************/
		  
	drop temporary table IF EXISTS   etl.vt_bak_datamapping_task;
	create temporary table etl.vt_bak_datamapping_task like etl.datamapping_task;
	
-- 插入当日备份数据到临时表 
	insert into etl.vt_bak_datamapping_task 
		select seq_num,
				t_tab_eng_name,
				t_tab_desc,
				etl_algorithm,
				physical_pri_key,
				etl_dev,
				sdm_dev,
				sdm_design,
				dev_process,
				enable,
				logical_pri_key,
				save_cycle,
				remarks,
				remarks1 from comm.bak_datamapping_task where cast(etl_date as date) = curdate();
			
		-- 支持重跑	
		 delete from etl.datamapping_task_contrast where cast(etl_date as date) = curdate();
		 
		 -- 备份表minus生产表
		 insert into etl.datamapping_task_contrast
		  select seq_num, t_tab_eng_name, t_tab_desc, etl_algorithm, physical_pri_key,
				etl_dev, sdm_dev, sdm_design, dev_process, enable, logical_pri_key, save_cycle,
				remarks, remarks1, curdate() 
		  from etl.vt_bak_datamapping_task 
		  MINUS
		  select seq_num, t_tab_eng_name, t_tab_desc, etl_algorithm, physical_pri_key,
				etl_dev, sdm_dev, sdm_design, dev_process, enable, logical_pri_key, save_cycle,
				remarks, remarks1, curdate() 
		  from etl.datamapping_task; 
		  
		  -- 生产表minus备份表
		  insert into etl.datamapping_task_contrast
		  select seq_num, t_tab_eng_name, t_tab_desc, etl_algorithm, physical_pri_key,
				etl_dev, sdm_dev, sdm_design, dev_process, enable, logical_pri_key, save_cycle,
				remarks, remarks1, curdate() 
		  from etl.datamapping_task 
		  MINUS
		  select seq_num, t_tab_eng_name, t_tab_desc, etl_algorithm, physical_pri_key,
				etl_dev, sdm_dev, sdm_design, dev_process, enable, logical_pri_key, save_cycle,
				remarks, remarks1, curdate() 
		  from etl.vt_bak_datamapping_task; 
		  /*
		  select * from datamapping_contrast t full on datamapping t1 
		  	on  t.t_tab_eng_name = t1.t_tab_eng_name
		  	and t.t_col_eng_name = t1.t_col_eng_name
		  	and t.s_group_id = t1.s_group_id ;
		  	*/
			
			
	set OUT_RES_MSG = '0'; 
END |