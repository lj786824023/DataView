CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_GUARANTY(
                                                            p_data_dt_str    IN    VARCHAR2,        --�������� yyyyMMdd
                                                            p_po_rtncode    OUT    VARCHAR2,        --���ر�� 1 �ɹ�,0 ʧ��
                                                            p_po_rtnmsg     OUT    VARCHAR2         --��������
                                                            )
  /*
    �洢��������:RWA_DEV.PRO_RWA_CD_GUARANTY
    ʵ�ֹ���:RWAϵͳ-�Ϲ����ݼ�-Ȩ�ط�/�������ϸ������϶�(��֤)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-04-21
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.RWA_EI_GUARANTEE|��֤��
    Դ  ��2 :RWA_DEV.RWA_EI_CLIENT|���������
    Դ  ��3 :RWA_DEV.RWA_EI_CMRELEVENCE|��ͬ�����������
    Դ  ��4 :RWA_DEV.RWA_EI_CONTRACT|��ͬ��
    Դ  ��5 :RWA_DEV.RWA_CD_GUARANTY_QUALIFIED|��֤�ϸ�ӳ���
    Դ  ��6 :RWA_DEV.RWA_CD_GUARANTY_TYPE|��֤������ʽ��
    Դ  ��7 :RWA_DEV.RWA_CD_GUARANTY_STD|�ϸ�֤Ȩ�ط�ӳ���
    Ŀ���  :RWA_DEV.RWA_EI_GUARANTEE|��֤��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
             QHJIANG|2017-04-17|���ݶ�������������Ӧ�ĸ���:����Ȩ�ط����¼����߼����µ������������������ļ����߼�
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_CD_GUARANTY';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;
  --����varchar2(4000)����
  type varchartab is table of varchar2(4000) index by binary_integer;
  v_sqlTab varchartab;
  --����������󳤶�
  v_tab_len INTEGER := 1;
  --����Ȩ�ط��ϸ��ʶ
  v_qualflagstd VARCHAR2(200) := '';
  --���������������ϸ��ʶ
  v_qualflagfirb VARCHAR2(200) := '';
  --����ִ�д����sql
  v_err_sql VARCHAR2(4000) := '';


  --�����α�,Ȩ�ط�/�������ϸ�֤�϶�����(��ѺƷĿ¼)
  cursor cc_qlifd is
      select
            --��֤���
            case when guaranty_type is not null
                 then ' AND T1.GUARANTEEWAY='''||guaranty_type||''' '
                 else ' ' end as guaranty_type
            --��������С�����
            ,case when client_sub_type is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.CLIENTSUBTYPE = ''' || client_sub_type || ''' AND T1.DATADATE = T2.DATADATE) '
                  else ' ' end as client_sub_type
            --ע����ҵ�����������
            ,case when country_level_no is not null and country_level_no = '01'--AA-��������
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING <= ''0104'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level_no is not null and country_level_no = '02'--AA-�����£�A-��������
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING > ''0104'' AND T2.RCERATING <= ''0107'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level_no is not null and country_level_no = '03'--A-�����£�BBB-��������
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING > ''0107'' AND T2.RCERATING <= ''0110'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level_no is not null and country_level_no = '04'--BBB-�����£�B-��������
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING > ''0110'' AND T2.RCERATING <= ''0116'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level_no is not null and country_level_no = '05'--B-������
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING > ''0116'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level_no is not null and country_level_no='06'--δ����
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND NVL(T2.RCERATING,''0124'') IN (''0124'',''0207'') AND T1.DATADATE = T2.DATADATE) '
                  when country_level_no is not null and country_level_no = '07'--A-��������
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING <= ''0107'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level_no is not null and country_level_no = '08'--BBB-��������
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING <= ''0110'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level_no is not null and country_level_no='09'--B-�������ϣ�A-������
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING <= ''0116'' AND T2.RCERATING>''0107'' AND T1.DATADATE = T2.DATADATE) '
                  else ' ' end as country_level_no
            --��֤������ʽ�Ƿ���ҵ����  ��ֵ:YesOrNo
            ,case when guarantee_form is not null and guarantee_form='1'
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.CLIENTSUBTYPE NOT LIKE ''04%'' AND T1.DATADATE = T2.DATADATE) '
                  when guarantee_form is not null and guarantee_form='0'
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.CLIENTSUBTYPE LIKE ''04%'' AND T1.DATADATE = T2.DATADATE) '
                  else '' end as guarantee_form
            --�Ƿ�֤��PD<�����PD  ��ֵ:YesOrNo
            ,case when pd_type is not null and pd_type='1'
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_TMP_GUARANTORPD T2 WHERE T1.GUARANTORID = T2.GUARANTORID AND T2.FLAG = ''1'') '
                  when pd_type is not null and pd_type='0'
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_TMP_GUARANTORPD T2 WHERE T1.GUARANTORID = T2.GUARANTORID AND T2.FLAG = ''0'') '

                  else '' end as pd_type
            ,qualified_flag
        from RWA_DEV.RWA_CD_GUARANTY_QUALIFIED order by serial_no asc;

  v_cc_qlifd cc_qlifd%rowtype;
  --�����α�,Ȩ�ط�/�������ϸ�֤���͹���(��ѺƷĿ¼)
  cursor cc_type is
      select guaranty_id,std_code,irb_code from RWA_DEV.RWA_CD_GUARANTY_TYPE ;
  v_cc_type cc_type%rowtype;

  --�����α�,Ȩ�ط�/�������ϸ�֤��ϸ�ֹ���(��ѺƷĿ¼)
  cursor cc_sdvs is
      select --��֤���
            case when guaranty_type is not null
                 then ' AND T1.GUARANTEEWAY='''||guaranty_type||''' '
                 else ' ' end as guaranty_type
            --��������С�����
            ,case when client_sub_type is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.CLIENTSUBTYPE = ''' || client_sub_type || ''' AND T1.DATADATE = T2.DATADATE) '
                  else ' ' end as client_sub_type
            --ע����ҵ�����������
            ,case when country_level is not null and country_level = '01'--AA-��������
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING <= ''0104'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level is not null and country_level = '02'--AA-�����£�A-��������
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING > ''0104'' AND T2.RCERATING <= ''0107'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level is not null and country_level = '03'--A-�����£�BBB-��������
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING > ''0107'' AND T2.RCERATING <= ''0110'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level is not null and country_level = '04'--BBB-�����£�B-��������
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING > ''0110'' AND T2.RCERATING <= ''0116'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level is not null and country_level = '05'--B-������
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING > ''0116'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level is not null and country_level='06'--δ����
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND NVL(T2.RCERATING,''0124'') IN (''0124'',''0207'') AND T1.DATADATE = T2.DATADATE) '
                  when country_level is not null and country_level = '07'--A-��������
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING <= ''0107'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level is not null and country_level = '08'--BBB-��������
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING <= ''0110'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level is not null and country_level = '09'--B-�������ϣ�A-������
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND AND T2.RCERATING <=''0116'' and T2.RCERATING>''0107'' AND T1.DATADATE = T2.DATADATE) '
                  else ' ' end as country_level
            ,guaranty_std_detail
        from RWA_DEV.RWA_CD_GUARANTY_STD order by serial_no asc;

  v_cc_sdvs cc_sdvs%rowtype;
  --�������sqlǰ׺
  v_sql_pre varchar2(4000);
  --�������where����
  v_clause varchar2(4000);

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    --��ձ�֤��������PD��ϵ��
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_GUARANTORPD';

    --���³�ʼ����֤��������PD��ϵ��
    INSERT INTO RWA_DEV.RWA_TMP_GUARANTORPD
	  (GUARANTORID, GUARANTORPD, CUSTOMERID, CUSTOMERPD, FLAG)
	  SELECT T1.GUARANTORID AS GUARANTORID,
	         T1.GUARANTORPD AS GUARANTORPD,
	         '' 						AS CUSTOMERID,
	         MIN(T5.PD) 		AS CUSTOMERPD,
	         CASE
	           WHEN T1.GUARANTORPD < MIN(T5.PD) THEN
	            '1'
	           ELSE
	            '0'
	         END AS FLAG
	    FROM RWA_EI_GUARANTEE T1
	   INNER JOIN RWA_EI_CMRELEVENCE T2
	      ON T1.GUARANTEEID = T2.MITIGATIONID
	     AND T1.DATADATE = T2.DATADATE
	   INNER JOIN RWA_EI_CONTRACT T3
	      ON T2.CONTRACTID = T3.CONTRACTID
	     AND T2.DATADATE = T3.DATADATE
	    LEFT JOIN RWA_EI_CLIENT T5
	      ON T3.CLIENTID = T5.CLIENTID
	     AND T3.DATADATE = T5.DATADATE
	   WHERE T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
	     AND T1.GUARANTORID IS NOT NULL
	   GROUP BY T1.GUARANTORID, T1.GUARANTORPD
		;

		COMMIT;
		--�������Ϣ
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TMP_GUARANTORPD',cascade => true);

    --1.���±�֤��ĺϸ��϶�״̬Ϊ��
    --v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE T1 SET T1.QUALFLAGSTD = '''',T1.GUARANTEETYPESTD='''',T1.GUARANTORSDVSSTD='''',T1.QUALFLAGFIRB='''',T1.GUARANTEETYPEIRB='''' WHERE T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    --������Ȩ�ط����֣���������ԭϵͳ��ȡ��RWAϵͳ������
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE T1 SET T1.QUALFLAGSTD = '''',T1.GUARANTEETYPESTD='''',T1.GUARANTORSDVSSTD='''' WHERE T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;

    --2.��ȡ��ִ�еĸ���sql
    --2.1 ��ȡ�ϸ��϶�sql
    for v_cc_qlifd in cc_qlifd loop
        v_qualflagstd := '';
        v_qualflagfirb := '';
        v_clause :=  v_cc_qlifd.guaranty_type     --��֤���
                   ||v_cc_qlifd.client_sub_type   --Ȩ�ط�.��������С�����
                   ||v_cc_qlifd.country_level_no  --Ȩ�ط�.ע����ҵ�����������
                   ||v_cc_qlifd.guarantee_form    --������.��֤������ʽ����ҵ���� �������� 01 ������ 02
                   ||v_cc_qlifd.pd_type;          --������.��֤��PD<�����PD �������� 01 ������ 02
        if v_cc_qlifd.qualified_flag = '01' then    --T Ȩ�ط����������ϸ� ����:QualificationFlag
            v_qualflagstd := '1';
            v_qualflagfirb := '1';
        elsif v_cc_qlifd.qualified_flag = '02' then --T1 Ȩ�ط����ϸ��������ϸ� ����:QualificationFlag
            v_qualflagstd := '0';
            v_qualflagfirb := '1';
        elsif v_cc_qlifd.qualified_flag = '03' then --F Ȩ�ط������������ϸ� ����:QualificationFlag
            v_qualflagstd := '0';
            v_qualflagfirb := '0';
        elsif v_cc_qlifd.qualified_flag = '04' then --T2 Ȩ�ط��ϸ����������ϸ� ����:QualificationFlag
            v_qualflagstd := '1';
            v_qualflagfirb := '0';
        else
            v_qualflagstd := '0';
            v_qualflagfirb := '0';
        end if;
        v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE T1
                                   SET T1.QUALFLAGSTD = '''||v_qualflagstd||'''
                                      ,T1.GUARANTEETYPESTD = ''''
                                      ,T1.GUARANTORSDVSSTD = ''''
                                      ,T1.QUALFLAGFIRB = NVL(T1.QUALFLAGFIRB,'''||v_qualflagfirb||''')
                                 WHERE T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'') 
                                 '||v_clause;
        v_tab_len := v_tab_len + 1;
    end loop;
    --���ʣ�������϶�δ���ϸ���sql
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE T1 SET T1.QUALFLAGSTD = ''0'', T1.GUARANTEETYPESTD = '''', T1.GUARANTORSDVSSTD = '''' WHERE T1.QUALFLAGSTD IS NULL AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE T1 SET T1.QUALFLAGFIRB = ''0'', T1.GUARANTEETYPEIRB = '''' WHERE T1.QUALFLAGFIRB IS NULL AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;

    --2.2 ��ȡ�Ѻϸ�֤����
    for v_cc_type in cc_type loop
        v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE T1 SET T1.GUARANTEETYPESTD = ''' || v_cc_type.std_code || ''' WHERE T1.GUARANTEEWAY = ''' || v_cc_type.guaranty_id || ''' AND T1.QUALFLAGSTD = ''1'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
        v_tab_len := v_tab_len + 1;
        v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE T1 SET T1.GUARANTEETYPEIRB = ''' || v_cc_type.irb_code || ''' WHERE T1.GUARANTEEWAY = ''' || v_cc_type.guaranty_id || ''' AND T1.QUALFLAGFIRB = ''1'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
        v_tab_len := v_tab_len + 1;
    end loop;


    --2.3 ��ȡ�Ѻϸ�֤��ϸ��
    for v_cc_sdvs in cc_sdvs loop
        v_clause :=  v_cc_sdvs.guaranty_type     --��֤���
                   ||v_cc_sdvs.client_sub_type   --Ȩ�ط�.��������С�����
                   ||v_cc_sdvs.country_level;    --Ȩ�ط�.ע����ҵ�����������

        v_sql_pre := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE T1 SET T1.GUARANTORSDVSSTD = ''' || v_cc_sdvs.guaranty_std_detail || ''' WHERE T1.QUALFLAGSTD = ''1''';

        v_sqlTab(v_tab_len) := v_sql_pre || v_clause || ' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
        v_tab_len := v_tab_len + 1;
    end loop;

    --3.ִ�и���sql
    --delete from qhjiang_test;
    for i in 1..v_sqlTab.count loop
        if v_sqlTab(i) IS NOT NULL THEN
            --insert into qhjiang_test(num1,str) values(i,v_sqlTab(i));
            --commit;
            v_err_sql := v_sqlTab(i);
            EXECUTE IMMEDIATE v_sqlTab(i);

            COMMIT;
        end if;
    end loop;

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_GUARANTEE WHERE (QUALFLAGSTD IS NOT NULL OR QUALFLAGFIRB IS NOT NULL) AND DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_GUARANTEE���϶�Ȩ�ط�/�������ºϸ�ı�֤���ݼ�¼Ϊ: ' || v_count || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
        --�����쳣
        EXCEPTION
    WHEN OTHERS THEN
                 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
                 Dbms_output.Put_line('������,����SQLΪ:'||v_err_sql);
            ROLLBACK;
                p_po_rtncode := sqlcode;
                p_po_rtnmsg  := 'Ȩ�ط�/�������ϸ�֤('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_CD_GUARANTY;
/

