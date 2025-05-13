CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_HGCOLLATERAL(
                                                                p_data_dt_str    IN    VARCHAR2,        --�������� yyyyMMdd
                                                                p_po_rtncode    OUT    VARCHAR2,        --���ر�� 1 �ɹ�,0 ʧ��
                                                                p_po_rtnmsg     OUT    VARCHAR2         --��������
                                                                )
  /*
    �洢��������:RWA_DEV.PRO_RWA_CD_HGCOLLATERAL
    ʵ�ֹ���:RWAϵͳ-�Ϲ����ݼ�-Ȩ�ط�/�������ع���ϸ������϶�(����ѺƷ)
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
    Դ  ��7 :RWA_DEV.CMS_BOND_INFO|ծȯ��Ϣ��
    Ŀ���1 :RWA_DEV.RWA_EI_COLLATERAL|����ѺƷ��
    Ŀ���2 :RWA_DEV.RWA_EI_SFTDETAIL|֤ȯ���ʽ�����Ϣ��
    �����¼(�޸���|�޸�ʱ��|�޸�����):

               TODO ������.��������������2 �е������ȼ�Ϊԭ�������ȼ���ת��
    */
  Authid Current_User
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_CD_HGCOLLATERAL';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;
  --����varchar2(4000)����
  type varchartab is table of varchar2(4000) index by binary_integer;
  v_sqlTab varchartab;
  --����������󳤶�
  v_tab_len INTEGER := 1;

  --v_beginTime BINARY_INTEGER;
  --v_endTime BINARY_INTEGER;
  --v_costTime varchar2(100);

  --�����α�,Ȩ�ط�/�������ع���ϸ������϶�����(��ѺƷĿ¼)
  cursor cc_qlifd is
      select
             --Ȩ�ط�/������.�����˲�������С������1
             case when client_type is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T2.DATADATE = T1.DATADATE AND T2.CLIENTID = T1.ISSUERID AND T2.CLIENTSUBTYPE = ''' || client_type || ''')'
                  else ' ' end as client_type_col
             --Ȩ�ط�/������.�����˲�������С������2
            ,case when client_type is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T2.DATADATE = T1.DATADATE AND T2.CLIENTID = T1.SECUISSUERID AND T2.CLIENTSUBTYPE = ''' || client_type || ''')'
                  else ' ' end as client_type_sft
             --Ȩ�ط�/������.��������������1
            ,case when collateral_type is not null and collateral_type = '01' --ծȯ
                  then ' AND T1.SOURCECOLTYPE = ''001003'''
                  when collateral_type is not null and collateral_type = '02' --Ʊ��
                  then ' AND T1.SOURCECOLTYPE = ''001004'''
                  else ' ' end as collateral_type_col
             --Ȩ�ط�/������.��������������2
            ,case when collateral_type is not null and collateral_type = '01' --ծȯ ֤ȯ���ʽ���ȫ��ծȯ
                  then ' AND 1 = 1'
                  when collateral_type is not null and collateral_type = '02' --Ʊ�� ֤ȯ���ʽ���û��Ʊ��
                  then ' AND 1 = 2'
                  else ' ' end as collateral_type_sft
             --Ȩ�ط�/������.ծȯ����Ŀ������1
            ,case when issue_intent_type is not null and issue_intent_type = '01' --�չ��������в�������
                  then ' AND T1.SPECPURPBONDFLAG = ''1'''
                  when issue_intent_type is not null and issue_intent_type = '02' --����
                  then ' AND T1.SPECPURPBONDFLAG = ''0'''
                  else ' ' end as issue_intent_type_col
             --Ȩ�ط�/������.ծȯ����Ŀ������2
            ,case when issue_intent_type is not null and issue_intent_type = '01' --�չ��������в�������
                  then ' AND T1.BONDISSUEINTENT = ''01'''
                  when issue_intent_type is not null and issue_intent_type = '02' --����
                  then ' AND T1.BONDISSUEINTENT = ''02'''
                  else ' ' end as issue_intent_type_sft
            --Ȩ�ط�.ע��ع�����������
            ,country_level_no
            --������.��������������1
            ,case when rating_result is not null and rating_result like '01%'
                  then ' AND T1.FCISSUERATING <= '''||rating_result||''' AND T1.FCISSUERATING like ''01%'' '
                  when rating_result is not null and rating_result like '02%'
                  then ' AND T1.FCISSUERATING <= '''||rating_result||''' AND T1.FCISSUERATING like ''02%'' '
                  else ' ' end as rating_result_col
            --������.��������������2
            ,case when rating_result is not null and rating_result like '01%'
                  then ' AND T1.SECUISSUERATING <= '''||rating_result||''' AND T1.SECUISSUERATING like ''01%'' '
                  when rating_result is not null and rating_result like '02%'
                  then ' AND T1.SECUISSUERATING <= '''||rating_result||''' AND T1.SECUISSUERATING like ''02%'' '
                  else ' ' end as rating_result_sft
            ,qualified_flag
       from RWA_DEV.RWA_CD_HGCOLLATERAL_QUALIFIED order by serial_no asc;

  v_cc_qlifd cc_qlifd%rowtype;
  --�����α�,Ȩ�ط�/�������ع���ϸ��������͹���(��ѺƷĿ¼)
  cursor cc_type is
      select
             --�����˲�������С������1
             case when client_type is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T2.DATADATE = T1.DATADATE AND T2.CLIENTID = T1.ISSUERID AND T2.CLIENTSUBTYPE = ''' || client_type || ''')'
                  else ' ' end as client_type_col
             --�����˲�������С������2
            ,case when client_type is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T2.DATADATE = T1.DATADATE AND T2.CLIENTID = T1.SECUISSUERID AND T2.CLIENTSUBTYPE = ''' || client_type || ''')'
                  else ' ' end as client_type_sft
             --��������������1
            ,case when collateral_type is not null and collateral_type = '01' --ծȯ
                  then ' AND T1.SOURCECOLTYPE = ''001003'''
                  when collateral_type is not null and collateral_type = '02' --Ʊ��
                  then ' AND T1.SOURCECOLTYPE = ''001004'''
                  else ' ' end as collateral_type_col
             --��������������2
            ,case when collateral_type is not null and collateral_type = '01' --ծȯ ֤ȯ���ʽ���ȫ��ծȯ
                  then ' AND 1 = 1'
                  when collateral_type is not null and collateral_type = '02' --Ʊ�� ֤ȯ���ʽ���û��Ʊ��
                  then ' AND 1 = 2'
                  else ' ' end as collateral_type_sft
            ,std_code
            ,irb_code
            ,irb_financial_code
        from RWA_DEV.RWA_CD_HGCOLLATERAL_TYPE;

  v_cc_type cc_type%rowtype;
  --�����α�,Ȩ�ط�/�������ع���ϸ�����ϸ�ֹ���(��ѺƷĿ¼)
  cursor cc_sdvs is
      select
             --Ȩ�ط�/������.�����˲�������С������1
             case when client_type is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T2.DATADATE = T1.DATADATE AND T2.CLIENTID = T1.ISSUERID AND T2.CLIENTSUBTYPE = ''' || client_type || ''')'
                  else ' ' end as client_type_col
             --Ȩ�ط�/������.�����˲�������С������2
            ,case when client_type is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T2.DATADATE = T1.DATADATE AND T2.CLIENTID = T1.SECUISSUERID AND T2.CLIENTSUBTYPE = ''' || client_type || ''')'
                  else ' ' end as client_type_sft
             --Ȩ�ط�/������.��������������1
            ,case when collateral_type is not null and collateral_type = '01' --ծȯ
                  then ' AND T1.SOURCECOLTYPE = ''001003'''
                  when collateral_type is not null and collateral_type = '02' --Ʊ��
                  then ' AND T1.SOURCECOLTYPE = ''001004'''
                  else ' ' end as collateral_type_col
             --Ȩ�ط�/������.��������������2
            ,case when collateral_type is not null and collateral_type = '01' --ծȯ ֤ȯ���ʽ���ȫ��ծȯ
                  then ' AND 1 = 1'
                  when collateral_type is not null and collateral_type = '02' --Ʊ�� ֤ȯ���ʽ���û��Ʊ��
                  then ' AND 1 = 2'
                  else ' ' end as collateral_type_sft
            --Ȩ�ط�.ע��ع�����������
            ,country_level
            ,collateral_std_detail
        from RWA_DEV.RWA_CD_HGCOLLATERAL_STD;

  v_cc_sdvs cc_sdvs%rowtype;

  --�������ѺƷ����sql
  v_col_sql varchar2(4000);
  --����֤ȯ���ʽ��׸���sql
  v_sft_sql varchar2(4000);

  --�������ѺƷ��������
  v_col_clause varchar2(4000);
  --����֤ȯ���ʽ��׸�������
  v_sft_clause varchar2(4000);

  --��������������������
  v_ratingClause varchar2(4000);

  --����Ȩ�ط��ϸ��ʶ
  v_qualflagstd varchar2(1);
  --�����������ϸ��ʶ
  v_qualflagfirb varchar2(1);
	--����ִ�д����sql
  v_err_sql VARCHAR2(4000) := '';

  BEGIN

    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
        --1.���»ع������ѺƷ��֤ȯ���ʽ�����Ϣ��ĺϸ��϶�״̬Ϊ��
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL SET QUALFLAGSTD = '''',COLLATERALTYPESTD = '''',COLLATERALSDVSSTD = '''',QUALFLAGFIRB='''',COLLATERALTYPEIRB = '''',FCTYPE = '''' WHERE SSYSID = ''HG'' AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;
    --���������뷵�ۻع����֤ȯ������
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_SFTDETAIL SET QUALFLAGSTD = '''',COLLATERALSDVSSTD = '''',QUALFLAGFIRB='''',FCTYPE = '''' WHERE SSYSID = ''HG'' AND SFTDETAILID LIKE ''%MRFSZQ'' AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;

    --2.��ȡ��ִ�еĸ���sql
    --2.1 ��ȡ�ϸ��϶�sql
    for v_cc_qlifd in cc_qlifd loop

        --ע��ع�����������
        if v_cc_qlifd.country_level_no = '01' then        --AA-��������
            v_ratingClause := ' AND T1.RCERATING <= ''0104'' ';
        elsif v_cc_qlifd.country_level_no = '02' then    --AA-�����£�A-��������
            v_ratingClause := ' AND T1.RCERATING > ''0104'' AND T1.RCERATING <= ''0107'' ';
        elsif v_cc_qlifd.country_level_no = '03' then    --A-�����£�BBB-��������
            v_ratingClause := ' AND T1.RCERATING > ''0107'' AND T2.RCERATING <= ''0110'' ';
        elsif v_cc_qlifd.country_level_no = '04' then    --BBB-�����£�B-��������
            v_ratingClause := ' AND T1.RCERATING > ''0110'' AND T2.RCERATING <= ''0116'' ';
        elsif v_cc_qlifd.country_level_no = '05' then    --B-������
            v_ratingClause := ' AND T1.RCERATING > ''0116'' ';
        elsif v_cc_qlifd.country_level_no = '06' then    --δ����
            v_ratingClause := ' AND NVL(T1.RCERATING,''0124'') IN (''0124'',''0207'') ';
        elsif v_cc_qlifd.country_level_no = '07' then    --A-��������
            v_ratingClause := ' AND T1.RCERATING <= ''0107'' ';
        elsif v_cc_qlifd.country_level_no = '08' then    --BBB-��������
            v_ratingClause := ' AND T1.RCERATING <= ''0110'' ';
        elsif v_cc_qlifd.country_level_no = '09' then    --B-�������ϣ�A-������
            v_ratingClause := ' AND T1.RCERATING <=''0116'' and T1.RCERATING > ''0107'' ';
        else --δ����������������
            v_ratingClause := ' ';
        end if;

        --�����˲�������С������
        v_col_clause :=  v_cc_qlifd.rating_result_col
                       ||v_cc_qlifd.issue_intent_type_col
                       ||v_cc_qlifd.client_type_col
                       ||v_ratingClause
                       ||v_cc_qlifd.collateral_type_col;
        v_sft_clause :=  v_cc_qlifd.rating_result_sft
                       ||v_cc_qlifd.issue_intent_type_sft
                       ||v_cc_qlifd.client_type_sft
                       ||v_ratingClause
                       ||v_cc_qlifd.collateral_type_sft;

        v_qualflagstd := '';
        v_qualflagfirb := '';
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

        --���»ع������ѺƷ���ϸ�
        v_col_sql := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL T1
                         SET T1.QUALFLAGSTD = '''||v_qualflagstd||'''
                            ,T1.QUALFLAGFIRB = '''||v_qualflagfirb||'''
                       WHERE T1.SSYSID = ''HG'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')'||v_col_clause ;
        --֤ȯ���ʽ��׽����뷵�ۻع�֤ȯ����Ҫ�϶����ʽ�˺������ع�֤ȯ��Ĭ��Ϊ�ϸ���ֽ����ʲ�
        v_sft_sql := 'UPDATE RWA_DEV.RWA_EI_SFTDETAIL T1
                         SET T1.QUALFLAGSTD = '''||v_qualflagstd||'''
                            ,T1.QUALFLAGFIRB = '''||v_qualflagfirb||'''
                       WHERE T1.SSYSID = ''HG'' AND T1.SFTDETAILID LIKE ''%MRFSZQ'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')'||v_sft_clause ;

        v_sqlTab(v_tab_len) := v_col_sql;
        v_tab_len := v_tab_len + 1;
        v_sqlTab(v_tab_len) := v_sft_sql;
        v_tab_len := v_tab_len + 1;
    end loop;

    --���ʣ�������϶�δ���ϸ���sql
    --Ȩ�ط�
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL SET QUALFLAGSTD = ''0'', COLLATERALTYPESTD = '''', COLLATERALSDVSSTD = '''' WHERE SSYSID = ''HG'' AND QUALFLAGSTD IS NULL AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;
    --������
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL SET QUALFLAGFIRB=''0'',COLLATERALTYPEIRB = '''',FCTYPE = '''' WHERE SSYSID = ''HG'' AND QUALFLAGFIRB IS NULL AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;
    --��֤ȯ����Ҫ�϶�
    --Ȩ�ط�
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_SFTDETAIL SET QUALFLAGSTD = ''0'', COLLATERALSDVSSTD = '''' WHERE SSYSID = ''HG'' AND SFTDETAILID LIKE ''%MRFSZQ'' AND QUALFLAGSTD IS NULL AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;
    --������
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_SFTDETAIL SET QUALFLAGFIRB = ''0'', FCTYPE = '''' WHERE SSYSID = ''HG'' AND SFTDETAILID LIKE ''%MRFSZQ'' AND QUALFLAGFIRB IS NULL AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;

    --2.2 ��ȡ�Ѻϸ����ѺƷ����
    for v_cc_type in cc_type loop
        v_col_clause :=  v_cc_type.collateral_type_col
                       ||v_cc_type.client_type_col||'';
        v_sft_clause :=  v_cc_type.collateral_type_sft
                       ||v_cc_type.client_type_sft||'';
        v_col_sql := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL T1 SET T1.COLLATERALTYPESTD = ''' || v_cc_type.std_code || ''' WHERE T1.SSYSID = ''HG'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'') AND T1.QUALFLAGSTD = ''1''';
        v_sft_sql := 'UPDATE RWA_DEV.RWA_EI_SFTDETAIL T1 SET T1.FCTYPE = ''' || v_cc_type.irb_financial_code || ''' WHERE T1.SSYSID = ''HG'' AND T1.SFTDETAILID LIKE ''%MRFSZQ'' AND T1.QUALFLAGFIRB = ''1'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';

        v_sqlTab(v_tab_len) := v_col_sql||v_col_clause;
        v_tab_len := v_tab_len + 1;
        v_sqlTab(v_tab_len) := v_sft_sql||v_sft_clause;
        v_tab_len := v_tab_len + 1;

        v_col_sql := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL T1 SET T1.COLLATERALTYPEIRB='''||v_cc_type.irb_code||''',T1.FCTYPE = ''' || v_cc_type.irb_financial_code || ''' WHERE T1.SSYSID = ''HG'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'') AND T1.QUALFLAGFIRB = ''1''';

        v_sqlTab(v_tab_len) := v_col_sql||v_col_clause;
        v_tab_len := v_tab_len + 1;
    end loop;

    --2.3 ��ȡ�Ѻϸ����ѺƷϸ��
    for v_cc_sdvs in cc_sdvs loop
        --ע��ع�����������
        if v_cc_sdvs.country_level = '01' then        --AA-��������
            v_ratingClause := ' AND T1.RCERATING <= ''0104'' ';
        elsif v_cc_sdvs.country_level = '02' then    --AA-�����£�A-��������
            v_ratingClause := ' AND T1.RCERATING > ''0104'' AND T1.RCERATING <= ''0107'' ';
        elsif v_cc_sdvs.country_level = '03' then    --A-�����£�BBB-��������
            v_ratingClause := ' AND T1.RCERATING > ''0107'' AND T1.RCERATING <= ''0110'' ';
        elsif v_cc_sdvs.country_level = '04' then    --BBB-�����£�B-��������
            v_ratingClause := ' AND T1.RCERATING > ''0110'' AND T1.RCERATING <= ''0116'' ';
        elsif v_cc_sdvs.country_level = '05' then    --B-������
            v_ratingClause := ' AND T1.RCERATING > ''0116'' ';
        elsif v_cc_sdvs.country_level = '06' then    --δ����
            v_ratingClause := ' AND NVL(T1.RCERATING,''0124'') IN (''0124'',''0207'') ';
        elsif v_cc_sdvs.country_level = '07' then    --A-��������
            v_ratingClause := ' AND T1.RCERATING <= ''0107'' ';
        elsif v_cc_sdvs.country_level = '08' then    --BBB-��������
            v_ratingClause := ' AND T1.RCERATING <= ''0110'' ';
        elsif v_cc_sdvs.country_level = '09' then    --B-�������ϣ�A-������
            v_ratingClause := ' AND T1.RCERATING <=''0116'' and T1.RCERATING > ''0107'' ';
        else --δ����������������
            v_ratingClause := ' ';
        end if;

        --�����˲�������С������
        v_col_clause :=  v_cc_sdvs.client_type_col
                       ||v_ratingClause
                       ||v_cc_sdvs.collateral_type_col;
        v_sft_clause :=  v_cc_sdvs.client_type_sft
                       ||v_ratingClause
                       ||v_cc_sdvs.collateral_type_sft;
        --����ǰ�����
        v_col_sql := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL T1 SET T1.COLLATERALSDVSSTD = ''' || v_cc_sdvs.collateral_std_detail || ''' WHERE T1.QUALFLAGSTD = ''1'' AND T1.SSYSID = ''HG'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')'||v_col_clause;
        --֤ȯ���ʽ��ײ��ϸ�Ҳ����ϸ��
        v_sft_sql := 'UPDATE RWA_DEV.RWA_EI_SFTDETAIL T1 SET T1.COLLATERALSDVSSTD = ''' || v_cc_sdvs.collateral_std_detail || ''' WHERE T1.QUALFLAGSTD = ''1'' AND T1.SSYSID = ''HG'' AND T1.SFTDETAILID LIKE ''%MRFSZQ'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')'||v_sft_clause;

        v_sqlTab(v_tab_len) := v_col_sql||v_col_clause;
        v_tab_len := v_tab_len + 1;
        v_sqlTab(v_tab_len) := v_sft_sql||v_sft_clause;
        v_tab_len := v_tab_len + 1;
    end loop;

    --3.ִ�и���sql
    --delete from qhjiang_test;
    for i in 1..v_sqlTab.count loop
        if v_sqlTab(i) IS NOT NULL THEN
            --insert into qhjiang_test(num1,str) values(i,v_sqlTab(i));
            --commit;
            --Dbms_output.Put_line(v_sqlTab(i));
            --v_beginTime := DBMS_UTILITY.GET_TIME;
            v_err_sql := v_sqlTab(i);
            EXECUTE IMMEDIATE v_sqlTab(i);
            --v_endTime := DBMS_UTILITY.GET_TIME;
            --v_costTime := v_endTime - v_beginTime;
            --update qhjiang_test set str1=v_costTime where num1=i;
            COMMIT;
        end if;
    end loop;

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_COLLATERAL WHERE QUALFLAGSTD = '1' AND SSYSID = 'HG' AND DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_COLLATERAL���϶�Ȩ�ط�/�������ºϸ�Ļع������ѺƷ���ݼ�¼Ϊ: ' || v_count || ' ��');

    SELECT COUNT(1)+v_count INTO v_count FROM RWA_DEV.RWA_EI_SFTDETAIL WHERE QUALFLAGSTD = '1' AND SSYSID = 'HG' AND SFTDETAILID LIKE '%MRFSZQ' AND DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_SFTDETAIL���϶�Ȩ�ط�/�������ºϸ�Ļع������ѺƷ���ݼ�¼Ϊ: ' || v_count || ' ��');


    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ6 ��:' || to_char(systimestamp, 'yyyy-mm-dd hh24:mi:ss.ff '));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
        --�����쳣
        EXCEPTION
    WHEN OTHERS THEN
                 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
                 Dbms_output.Put_line(v_err_sql);
            ROLLBACK;
                p_po_rtncode := sqlcode;
                p_po_rtnmsg  := 'Ȩ�ط�/�������ع���ϸ����ѺƷ('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_CD_HGCOLLATERAL;
/

