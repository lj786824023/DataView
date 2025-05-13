CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_GUARANTY_STD(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_CD_GUARANTY_STD
    ʵ�ֹ���:RWAϵͳ-�Ϲ����ݼ�-Ȩ�ط��ϸ������϶�(��֤)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-04-21
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.RWA_EI_GUARANTEE|��֤��
    Դ  ��2 :RWA_DEV.RWA_EI_CLINET|���������
    Դ  ��3 :RWA_DEV.RWA_CD_GUARANTY_QUALIFIED|��֤�ϸ�ӳ���
    Դ  ��3 :RWA_DEV.RWA_CD_GUARANTY_TYPE|��֤������ʽ��
    Դ  ��3 :RWA_DEV.RWA_CD_GUARANTY_STD|�ϸ�֤Ȩ�ط�ӳ���
    Ŀ���  :RWA_DEV.RWA_EI_GUARANTEE|��֤��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_CD_GUARANTY_STD';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;
  --����varchar2(4000)����
  type varchartab is table of varchar2(4000) index by binary_integer;
  v_sqlTab varchartab;
  --����������󳤶�
  v_tab_len INTEGER := 1;
  --�����α�,Ȩ�ط��ϸ�֤�϶�����(��ѺƷĿ¼)
  cursor cc_qlifd is
  	select guaranty_type,client_sub_type,country_level_no,qualified_flag from RWA_DEV.RWA_CD_GUARANTY_QUALIFIED;

  v_cc_qlifd cc_qlifd%rowtype;
  --�����α�,Ȩ�ط��ϸ�֤���͹���(��ѺƷĿ¼)
  --cursor cc_type is
  --	select guaranty_id,std_code from RWA_DEV.RWA_CD_GUARANTY_TYPE;

  --v_cc_type cc_type%rowtype;
  --�����α�,Ȩ�ط��ϸ�֤��ϸ�ֹ���(��ѺƷĿ¼)
  cursor cc_sdvs is
  	select guaranty_type,client_sub_type,country_level,guaranty_std_detail from RWA_DEV.RWA_CD_GUARANTY_STD;

  v_cc_sdvs cc_sdvs%rowtype;
  --�������sqlǰ׺
  v_sql_pre varchar2(4000);
  --�������where����
  v_clause varchar2(4000);

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --1.���±�֤��ĺϸ��϶�״̬Ϊ��
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE SET QUALFLAGSTD = '''' WHERE DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;

    --2.��ȡ��ִ�еĸ���sql
    --2.1 ��ȡ�ϸ��϶�sql
    for v_cc_qlifd in cc_qlifd loop
    	if v_cc_qlifd.qualified_flag <> '01' then
    		v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE SET QUALFLAGSTD = ''0'', GUARANTEETYPESTD = '''', GUARANTORSDVSSTD = '''' WHERE GUARANTEEWAY = ''' || v_cc_qlifd.guaranty_type || ''' AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    	else
    		v_sql_pre := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE T1 SET T1.QUALFLAGSTD = ''1'', T1.GUARANTEETYPESTD = ''020101'', T1.GUARANTORSDVSSTD = '''' WHERE T1.GUARANTEEWAY = ''' || v_cc_qlifd.guaranty_type || '''';
    		v_clause := '';
    		if v_cc_qlifd.client_sub_type is not null then
    			v_clause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.CLIENTSUBTYPE = ''' || v_cc_qlifd.client_sub_type || ''' AND T2.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
	    		if v_cc_qlifd.country_level_no = '01' then		--AA-��������
	    			v_clause := v_clause || ' AND T2.RCERATING <= ''0104'')';
	    		elsif v_cc_qlifd.country_level_no = '02' then	--AA-�����£�A-��������
	    			v_clause := v_clause || ' AND T2.RCERATING > ''0104'' AND T2.RCERATING <= ''0107'')';
	    		elsif v_cc_qlifd.country_level_no = '03' then	--A-�����£�BBB-��������
	    			v_clause := v_clause || ' AND T2.RCERATING > ''0107'' AND T2.RCERATING <= ''0110'')';
	    		elsif v_cc_qlifd.country_level_no = '04' then	--BBB-�����£�B-��������
	    			v_clause := v_clause || ' AND T2.RCERATING > ''0110'' AND T2.RCERATING <= ''0116'')';
	    		elsif v_cc_qlifd.country_level_no = '05' then	--B-������
	    			v_clause := v_clause || ' AND T2.RCERATING > ''0116'')';
	    		elsif v_cc_qlifd.country_level_no = '07' then	--A-��������
	    			v_clause := v_clause || ' AND T2.RCERATING <= ''0107'')';
	    		elsif v_cc_qlifd.country_level_no = '08' then	--BBB-��������
	    			v_clause := v_clause || ' AND T2.RCERATING <= ''0110'')';
	    		else																					--δ����������������
	    			v_clause := v_clause || ')';
	    		end if;
    		else
    			v_clause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    			if v_cc_qlifd.country_level_no = '01' then		--AA-��������
	    			v_clause := v_clause || ' AND T2.RCERATING <= ''0104'')';
	    		elsif v_cc_qlifd.country_level_no = '02' then	--AA-�����£�A-��������
	    			v_clause := v_clause || ' AND T2.RCERATING > ''0104'' AND T2.RCERATING <= ''0107'')';
	    		elsif v_cc_qlifd.country_level_no = '03' then	--A-�����£�BBB-��������
	    			v_clause := v_clause || ' AND T2.RCERATING > ''0107'' AND T2.RCERATING <= ''0110'')';
	    		elsif v_cc_qlifd.country_level_no = '04' then	--BBB-�����£�B-��������
	    			v_clause := v_clause || ' AND T2.RCERATING > ''0110'' AND T2.RCERATING <= ''0116'')';
	    		elsif v_cc_qlifd.country_level_no = '05' then	--B-������
	    			v_clause := v_clause || ' AND T2.RCERATING > ''0116'')';
	    		elsif v_cc_qlifd.country_level_no = '07' then	--A-��������
	    			v_clause := v_clause || ' AND T2.RCERATING <= ''0107'')';
	    		elsif v_cc_qlifd.country_level_no = '08' then	--BBB-��������
	    			v_clause := v_clause || ' AND T2.RCERATING <= ''0110'')';
	    		else																					--δ����������������
	    			v_clause := '';
	    		end if;
    		end if;
    		v_sqlTab(v_tab_len) := v_sql_pre || v_clause || ' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    	end if;
    	v_tab_len := v_tab_len + 1;
    end loop;
    --���ʣ�������϶�δ���ϸ���sql
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE SET QUALFLAGSTD = ''0'', GUARANTEETYPESTD = '''', GUARANTORSDVSSTD = '''' WHERE QUALFLAGSTD IS NULL AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;

    --2.2 ��ȡ�Ѻϸ�֤����
    /**��֤���͹̶�������������ñ��л�ȡ
    for v_cc_type in cc_type loop
    	v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE SET GUARANTEETYPESTD = ''' || v_cc_type.std_code || ''' WHERE GUARANTEEWAY = ''' || v_cc_type.guaranty_id || ''' AND QUALFLAGSTD = ''1'' AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    	v_tab_len := v_tab_len + 1;
    end loop;
    */

    --2.3 ��ȡ�Ѻϸ�֤��ϸ��
    for v_cc_sdvs in cc_sdvs loop
    	v_sql_pre := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE T1 SET T1.GUARANTORSDVSSTD = ''' || v_cc_sdvs.guaranty_std_detail || ''' WHERE T1.GUARANTEEWAY = ''' || v_cc_sdvs.guaranty_type || ''' AND T1.QUALFLAGSTD = ''1''';
    	v_clause := '';
  		if v_cc_sdvs.client_sub_type is not null then
  			v_clause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.CLIENTSUBTYPE = ''' || v_cc_sdvs.client_sub_type || ''' AND T2.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    		if v_cc_sdvs.country_level = '01' then		--AA-��������
    			v_clause := v_clause || ' AND T2.RCERATING <= ''0104'')';
    		elsif v_cc_sdvs.country_level = '02' then	--AA-�����£�A-��������
    			v_clause := v_clause || ' AND T2.RCERATING > ''0104'' AND T2.RCERATING <= ''0107'')';
    		elsif v_cc_sdvs.country_level = '03' then	--A-�����£�BBB-��������
    			v_clause := v_clause || ' AND T2.RCERATING > ''0107'' AND T2.RCERATING <= ''0110'')';
    		elsif v_cc_sdvs.country_level = '04' then	--BBB-�����£�B-��������
    			v_clause := v_clause || ' AND T2.RCERATING > ''0110'' AND T2.RCERATING <= ''0116'')';
    		elsif v_cc_sdvs.country_level = '05' then	--B-������
    			v_clause := v_clause || ' AND T2.RCERATING > ''0116'')';
    		elsif v_cc_sdvs.country_level = '07' then	--A-��������
    			v_clause := v_clause || ' AND T2.RCERATING <= ''0107'')';
    		elsif v_cc_sdvs.country_level = '08' then	--BBB-��������
    			v_clause := v_clause || ' AND T2.RCERATING <= ''0110'')';
    		else																			--δ����������������
    			v_clause := v_clause || ')';
    		end if;
  		else
  			v_clause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
  			if v_cc_sdvs.country_level = '01' then		--AA-��������
    			v_clause := v_clause || ' AND T2.RCERATING <= ''0104'')';
    		elsif v_cc_sdvs.country_level = '02' then	--AA-�����£�A-��������
    			v_clause := v_clause || ' AND T2.RCERATING > ''010104'' AND T2.RCERATING <= ''0107'')';
    		elsif v_cc_sdvs.country_level = '03' then	--A-�����£�BBB-��������
    			v_clause := v_clause || ' AND T2.RCERATING > ''0107'' AND T2.RCERATING <= ''0110'')';
    		elsif v_cc_sdvs.country_level = '04' then	--BBB-�����£�B-��������
    			v_clause := v_clause || ' AND T2.RCERATING > ''0110'' AND T2.RCERATING <= ''0116'')';
    		elsif v_cc_sdvs.country_level = '05' then	--B-������
    			v_clause := v_clause || ' AND T2.RCERATING > ''0116'')';
    		elsif v_cc_sdvs.country_level = '07' then	--A-��������
    			v_clause := v_clause || ' AND T2.RCERATING <= ''0107'')';
    		elsif v_cc_sdvs.country_level = '08' then	--BBB-��������
    			v_clause := v_clause || ' AND T2.RCERATING <= ''0110'')';
    		else																			--δ����������������
    			v_clause := '';
    		end if;
  		end if;
    	v_sqlTab(v_tab_len) := v_sql_pre || v_clause || ' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    	v_tab_len := v_tab_len + 1;
    end loop;

    --3.ִ�и���sql
    for i in 1..v_sqlTab.count loop
    	if v_sqlTab(i) IS NOT NULL THEN
	    	EXECUTE IMMEDIATE v_sqlTab(i);

	    	COMMIT;
    	end if;
    end loop;

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_GUARANTEE WHERE QUALFLAGSTD = '1' AND DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_GUARANTEE���϶�Ȩ�ط��ºϸ�ı�֤���ݼ�¼Ϊ: ' || v_count || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := 'Ȩ�ط��ϸ�֤('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_CD_GUARANTY_STD;
/

