CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_HGCOLLATERAL_STD(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_CD_HGCOLLATERAL_STD
    ʵ�ֹ���:RWAϵͳ-�Ϲ����ݼ�-Ȩ�ط��ع���ϸ������϶�(����ѺƷ)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-04-21
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.RWA_EI_COLLATERAL|����ѺƷ��
    Դ  ��2 :RWA_DEV.RWA_EI_SFTDETAIL|֤ȯ���ʽ�����Ϣ��
    Դ  ��3 :RWA_DEV.RWA_CD_HGCOLLATERAL_QUALIFIED|�ع������ѺƷ�ϸ�ӳ���
    Դ  ��4 :RWA_DEV.RWA_CD_HGCOLLATERAL_TYPE|�ع������ѺƷ���ʹ����
    Դ  ��5 :RWA_DEV.RWA_CD_HGCOLLATERAL_STD|�ϸ�ع������ѺƷȨ�ط�ӳ���
    Դ  ��6 :RWA_DEV.RWA_EI_CLIENT|����������ܱ�
    Դ  ��7 :RWA_DEV.RWA_EI_EXPOSURE|���ձ�¶���ܱ�
    Ŀ���1 :RWA_DEV.RWA_EI_COLLATERAL|����ѺƷ��
    Ŀ���2 :RWA_DEV.RWA_EI_SFTDETAIL|֤ȯ���ʽ�����Ϣ��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  Authid Current_User
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_CD_HGCOLLATERAL_STD';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;
  v_count1 INTEGER;
  --����varchar2(4000)����
  type varchartab is table of varchar2(4000) index by binary_integer;
  v_sqlTab varchartab;
  --����������󳤶�
  v_tab_len INTEGER := 1;
  --�����α�,Ȩ�ط��ع���ϸ������϶�����(��ѺƷĿ¼)
  cursor cc_qlifd is
  	select client_type,collateral_type,issue_intent_type,country_level_no,qualified_flag from RWA_DEV.RWA_CD_HGCOLLATERAL_QUALIFIED;

  v_cc_qlifd cc_qlifd%rowtype;
  --�����α�,Ȩ�ط��ع���ϸ��������͹���(��ѺƷĿ¼)
  cursor cc_type is
  	select client_type,collateral_type,std_code from RWA_DEV.RWA_CD_HGCOLLATERAL_TYPE;

  v_cc_type cc_type%rowtype;
  --�����α�,Ȩ�ط��ع���ϸ�����ϸ�ֹ���(��ѺƷĿ¼)
  cursor cc_sdvs is
  	select client_type,collateral_type,country_level,collateral_std_detail from RWA_DEV.RWA_CD_HGCOLLATERAL_STD;

  v_cc_sdvs cc_sdvs%rowtype;

  --�������ѺƷ����sql
	v_col_sql varchar2(4000);
	--����֤ȯ���ʽ��׸���sql
	v_sft_sql varchar2(4000);
	--�������ѺƷ����sqlǰ׺
	v_col_sql_pre varchar2(4000);
	--����֤ȯ���ʽ��׸���sqlǰ׺
	v_sft_sql_pre varchar2(4000);
	--�������ѺƷ��������������
	v_colClientClause varchar2(4000);
	--����֤ȯ���ʽ��׷�������������
	v_sftClientClause varchar2(4000);
	--�������ѺƷ��������������
	v_col_colTypeClause varchar2(4000);
	--����֤ȯ���ʽ��׻�������������
	v_sft_colTypeClause varchar2(4000);
	--�������ѺƷծȯ����Ŀ������
	v_col_intentClause varchar2(4000);
	--����֤ȯ���ʽ���ծȯ����Ŀ������
	v_sft_intentClause varchar2(4000);
	--�������ѺƷ�����˹�����������
	v_col_ratingClause varchar2(4000);
	--����֤ȯ���ʽ��׷����˹�����������
	v_sft_ratingClause varchar2(4000);

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
		--1.���»ع������ѺƷ��֤ȯ���ʽ�����Ϣ��ĺϸ��϶�״̬Ϊ��
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL SET QUALFLAGSTD = '''' WHERE SSYSID = ''HG'' AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;
    --���������뷵�ۻع����֤ȯ������
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_SFTDETAIL SET QUALFLAGSTD = '''' WHERE SSYSID = ''HG'' AND SFTDETAILID LIKE ''%MRFSZQ'' AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;

    --2.��ȡ��ִ�еĸ���sql
    --2.1 ��ȡ�ϸ��϶�sql
    for v_cc_qlifd in cc_qlifd loop
    	--�����˲�������С������
    	v_colClientClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T2.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'') AND T2.CLIENTID = T1.ISSUERID AND T2.CLIENTSUBTYPE = ''' || v_cc_qlifd.client_type || '''';
    	v_sftClientClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T2.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'') AND T2.CLIENTID = T1.SECUISSUERID AND T2.CLIENTSUBTYPE = ''' || v_cc_qlifd.client_type || '''';
    	if v_cc_qlifd.qualified_flag <> '01' then --���ϸ�
    		--��������������
    		if v_cc_qlifd.collateral_type = '01' then			--ծȯ
    			v_col_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.SGUARCONTRACTID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111103%'' OR T3.ACCSUBJECT1 LIKE ''211103%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
    			v_sft_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.EXPOSUREID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111103%'' OR T3.ACCSUBJECT1 LIKE ''211103%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
    		elsif v_cc_qlifd.collateral_type = '02' then	--Ʊ��
    			v_col_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.SGUARCONTRACTID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111102%'' OR T3.ACCSUBJECT1 LIKE ''211102%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
    			v_sft_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.EXPOSUREID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111102%'' OR T3.ACCSUBJECT1 LIKE ''211102%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
    		else																					--�����޴�����
    			v_col_colTypeClause := '';
    			v_sft_colTypeClause := '';
    		end if;
    		--���»ع������ѺƷ���ϸ�
    		v_col_sql_pre := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL T1 SET T1.QUALFLAGSTD = ''0'', T1.COLLATERALTYPESTD = '''', T1.COLLATERALSDVSSTD = '''' WHERE T1.SSYSID = ''HG'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')' || v_colClientClause || ')' || v_col_colTypeClause;
    		--֤ȯ���ʽ��׽����뷵�ۻع�֤ȯ����Ҫ�϶����ʽ�˺������ع�֤ȯ��Ĭ��Ϊ�ϸ���ֽ����ʲ�
    		v_sft_sql_pre := 'UPDATE RWA_DEV.RWA_EI_SFTDETAIL T1 SET T1.QUALFLAGSTD = ''0'', T1.COLLATERALSDVSSTD = '''' WHERE T1.SSYSID = ''HG'' AND T1.SFTDETAILID LIKE ''%MRFSZQ'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')' || v_sftClientClause || ')' || v_sft_colTypeClause;

    		v_col_sql := v_col_sql_pre;
    		v_sft_sql := v_sft_sql_pre;
    	else																			--�ϸ�
    		v_col_sql_pre := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL T1 SET T1.QUALFLAGSTD = ''1'' WHERE T1.SSYSID = ''HG'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    		v_sft_sql_pre := 'UPDATE RWA_DEV.RWA_EI_SFTDETAIL T1 SET T1.QUALFLAGSTD = ''1'' WHERE T1.SSYSID = ''HG'' AND T1.SFTDETAILID LIKE ''%MRFSZQ'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    		--ע��ع�����������
    		if v_cc_qlifd.country_level_no = '01' then		--AA-��������
    			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING <= ''0104'')';
    			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING <= ''0104'')';
    		elsif v_cc_qlifd.country_level_no = '02' then	--AA-�����£�A-��������
    			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING > ''0104'' AND T2.RCERATING <= ''0107'')';
    			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING > ''0104'' AND T2.RCERATING <= ''0107'')';
    		elsif v_cc_qlifd.country_level_no = '03' then	--A-�����£�BBB-��������
    			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING > ''0107'' AND T2.RCERATING <= ''0110'')';
    			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING > ''0107'' AND T2.RCERATING <= ''0110'')';
    		elsif v_cc_qlifd.country_level_no = '04' then	--BBB-�����£�B-��������
    			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING > ''0110'' AND T2.RCERATING <= ''0116'')';
    			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING > ''0110'' AND T2.RCERATING <= ''0116'')';
    		elsif v_cc_qlifd.country_level_no = '05' then	--B-������
    			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING > ''0116'')';
    			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING > ''0116'')';
    		elsif v_cc_qlifd.country_level_no = '07' then	--A-��������
    			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING <= ''0107'')';
    			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING <= ''0107'')';
    		elsif v_cc_qlifd.country_level_no = '08' then	--BBB-��������
    			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING <= ''0110'')';
    			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING <= ''0110'')';
    		else																					--δ����������������
    			v_col_ratingClause := v_colClientClause || ')';
    			v_sft_ratingClause := v_sftClientClause || ')';
    		end if;
    		--��������������
    		if v_cc_qlifd.collateral_type = '01' then			--ծȯ
    			v_col_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.SGUARCONTRACTID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111103%'' OR T3.ACCSUBJECT1 LIKE ''211103%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
    			v_sft_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.EXPOSUREID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111103%'' OR T3.ACCSUBJECT1 LIKE ''211103%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
    		elsif v_cc_qlifd.collateral_type = '02' then	--Ʊ��
    			v_col_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.SGUARCONTRACTID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111102%'' OR T3.ACCSUBJECT1 LIKE ''211102%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
    			v_sft_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.EXPOSUREID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111102%'' OR T3.ACCSUBJECT1 LIKE ''211102%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
    		else																					--�����޴�����
    			v_col_colTypeClause := '';
    			v_sft_colTypeClause := '';
    		end if;
    		--ծȯ����Ŀ������
    		if v_cc_qlifd.issue_intent_type = '01' then			--�չ��������в�������
    			v_col_intentClause := ' AND T1.SPECPURPBONDFLAG = ''1''';
    			v_sft_intentClause := ' AND T1.BONDISSUEINTENT = ''01''';
    		elsif v_cc_qlifd.issue_intent_type = '02' then	--����
    			v_col_intentClause := ' AND T1.SPECPURPBONDFLAG = ''0''';
    			v_sft_intentClause := ' AND T1.BONDISSUEINTENT = ''02''';
    		else																						--�޴�����
    			v_col_intentClause := '';
    			v_sft_intentClause := '';
    		end if;

    		v_col_sql := v_col_sql_pre || v_col_ratingClause || v_col_colTypeClause || v_col_intentClause;
    		v_sft_sql := v_sft_sql_pre || v_sft_ratingClause || v_sft_colTypeClause || v_sft_intentClause;
    	end if;
    	v_sqlTab(v_tab_len) := v_col_sql;
    	v_tab_len := v_tab_len + 1;
    	v_sqlTab(v_tab_len) := v_sft_sql;
    	v_tab_len := v_tab_len + 1;
    end loop;
    --���ʣ�������϶�δ���ϸ���sql
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL SET QUALFLAGSTD = ''0'', COLLATERALTYPESTD = '''', COLLATERALSDVSSTD = '''' WHERE SSYSID = ''HG'' AND QUALFLAGSTD IS NULL AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;
    --��֤ȯ����Ҫ�϶�
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_SFTDETAIL SET QUALFLAGSTD = ''0'', COLLATERALSDVSSTD = '''' WHERE SSYSID = ''HG'' AND SFTDETAILID LIKE ''%MRFSZQ'' AND QUALFLAGSTD IS NULL AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;

    --2.2 ��ȡ�Ѻϸ����ѺƷ����
    for v_cc_type in cc_type loop
    	v_col_sql_pre := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL T1 SET T1.COLLATERALTYPESTD = ''' || v_cc_type.std_code || ''' WHERE T1.SSYSID = ''HG'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'') AND T1.QUALFLAGSTD = ''1''';
    	v_colClientClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T2.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'') AND T2.CLIENTID = T1.ISSUERID AND T2.CLIENTSUBTYPE = ''' || v_cc_type.client_type || ''')';
    	--��������������
  		if v_cc_type.collateral_type = '01' then			--ծȯ
  			v_col_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.SGUARCONTRACTID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111103%'' OR T3.ACCSUBJECT1 LIKE ''211103%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
  		elsif v_cc_type.collateral_type = '02' then		--Ʊ��
  			v_col_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.SGUARCONTRACTID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111102%'' OR T3.ACCSUBJECT1 LIKE ''211102%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
  		else																					--�����޴�����
  			v_col_colTypeClause := '';
  		end if;
  		v_col_sql := v_col_sql_pre || v_colClientClause || v_col_colTypeClause;
    	v_sqlTab(v_tab_len) := v_col_sql;
    	v_tab_len := v_tab_len + 1;
    end loop;

    --2.3 ��ȡ�Ѻϸ����ѺƷϸ��
    for v_cc_sdvs in cc_sdvs loop
    	--�����˲�������С������
    	v_colClientClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T2.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'') AND T2.CLIENTID = T1.ISSUERID AND T2.CLIENTSUBTYPE = ''' || v_cc_sdvs.client_type || '''';
    	v_sftClientClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T2.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'') AND T2.CLIENTID = T1.SECUISSUERID AND T2.CLIENTSUBTYPE = ''' || v_cc_sdvs.client_type || '''';
    	--����ǰ�����
    	v_col_sql_pre := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL T1 SET T1.COLLATERALSDVSSTD = ''' || v_cc_sdvs.collateral_std_detail || ''' WHERE T1.QUALFLAGSTD = ''1'' AND T1.SSYSID = ''HG'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    	--֤ȯ���ʽ��ײ��ϸ�Ҳ����ϸ��
    	v_sft_sql_pre := 'UPDATE RWA_DEV.RWA_EI_SFTDETAIL T1 SET T1.COLLATERALSDVSSTD = ''' || v_cc_sdvs.collateral_std_detail || ''' WHERE T1.SSYSID = ''HG'' AND T1.SFTDETAILID LIKE ''%MRFSZQ'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
			--ע��ع�����������
  		if v_cc_sdvs.country_level = '01' then				--AA-��������
  			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING <= ''0104'')';
  			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING <= ''0104'')';
  		elsif v_cc_sdvs.country_level = '02' then			--AA-�����£�A-��������
  			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING > ''0104'' AND T2.RCERATING <= ''0107'')';
  			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING > ''0104'' AND T2.RCERATING <= ''0107'')';
  		elsif v_cc_sdvs.country_level = '03' then			--A-�����£�BBB-��������
  			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING > ''0107'' AND T2.RCERATING <= ''0110'')';
  			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING > ''0107'' AND T2.RCERATING <= ''0110'')';
  		elsif v_cc_sdvs.country_level = '04' then			--BBB-�����£�B-��������
  			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING > ''0110'' AND T2.RCERATING <= ''0116'')';
  			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING > ''0110'' AND T2.RCERATING <= ''0116'')';
  		elsif v_cc_sdvs.country_level = '05' then			--B-������
  			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING > ''0116'')';
  			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING > ''0116'')';
  		elsif v_cc_sdvs.country_level = '07' then			--A-��������
  			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING <= ''0107'')';
  			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING <= ''0107'')';
  		elsif v_cc_sdvs.country_level = '08' then			--BBB-��������
  			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING <= ''0110'')';
  			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING <= ''0110'')';
  		else																					--����������
  			v_col_ratingClause := v_colClientClause || ')';
  			v_sft_ratingClause := v_sftClientClause || ')';
  		end if;
  		--��������������
  		if v_cc_sdvs.collateral_type = '01' then			--ծȯ
  			v_col_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.SGUARCONTRACTID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111103%'' OR T3.ACCSUBJECT1 LIKE ''211103%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
  			v_sft_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.EXPOSUREID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111103%'' OR T3.ACCSUBJECT1 LIKE ''211103%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
  		elsif v_cc_sdvs.collateral_type = '02' then		--Ʊ��
  			v_col_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.SGUARCONTRACTID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111102%'' OR T3.ACCSUBJECT1 LIKE ''211102%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
  			v_sft_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.EXPOSUREID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111102%'' OR T3.ACCSUBJECT1 LIKE ''211102%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
  		else																					--�����޴�����
  			v_col_colTypeClause := '';
  			v_sft_colTypeClause := '';
  		end if;
    	v_col_sql := v_col_sql_pre || v_col_ratingClause || v_col_colTypeClause;
    	v_sft_sql := v_sft_sql_pre || v_sft_ratingClause || v_sft_colTypeClause;
    	v_sqlTab(v_tab_len) := v_col_sql;
    	v_tab_len := v_tab_len + 1;
    	v_sqlTab(v_tab_len) := v_sft_sql;
    	v_tab_len := v_tab_len + 1;
    end loop;

    --3.ִ�и���sql
    for i in 1..v_sqlTab.count loop
    	if v_sqlTab(i) IS NOT NULL THEN
    		--Dbms_output.Put_line(v_sqlTab(i));
	    	EXECUTE IMMEDIATE v_sqlTab(i);

	    	COMMIT;
    	end if;
    end loop;

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_COLLATERAL WHERE QUALFLAGSTD = '1' AND SSYSID = 'HG' AND DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_COLLATERAL���϶�Ȩ�ط��ºϸ�Ļع������ѺƷ���ݼ�¼Ϊ: ' || v_count || ' ��');

    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_EI_SFTDETAIL WHERE QUALFLAGSTD = '1' AND SSYSID = 'HG' AND SFTDETAILID LIKE '%MRFSZQ' AND DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_SFTDETAIL���϶�Ȩ�ط��ºϸ�Ļع������ѺƷ���ݼ�¼Ϊ: ' || v_count || ' ��');


    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || (v_count + v_count1);
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := 'Ȩ�ط��ع���ϸ����ѺƷ('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_CD_HGCOLLATERAL_STD;
/

