CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_COLLATERAL_STD(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_CD_COLLATERAL_STD
    ʵ�ֹ���:RWAϵͳ-�Ϲ����ݼ�-Ȩ�ط��ϸ������϶�(����ѺƷ)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-04-21
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.RWA_EI_COLLATERAL|����ѺƷ��
    Դ  ��2 :RWA_DEV.RWA_CD_COLLATERAL_QUALIFIED|����ѺƷ�ϸ�ӳ���
    Դ  ��3 :RWA_DEV.RWA_CD_COLLATERAL_TYPE|����ѺƷ���ʹ����
    Դ  ��4 :RWA_DEV.RWA_CD_COLLATERAL_STD|�ϸ����ѺƷȨ�ط�ӳ���
    Ŀ���  :RWA_DEV.RWA_EI_COLLATERAL|����ѺƷ��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  Authid Current_User
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_CD_COLLATERAL_STD';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;
  --����varchar2(4000)����
  type varchartab is table of varchar2(4000) index by binary_integer;
  v_sqlTab varchartab;
  --����������󳤶�
  v_tab_len INTEGER := 1;
  --�����α�,Ȩ�ط��ϸ������϶�����(��ѺƷĿ¼)
  cursor cc_qlifd is
  	select guaranty_kind2, --ѺƷ�������
           qualified_flag  --�ϸ��ʶ ��ֵ:QualificationFlag
      from RWA_DEV.RWA_CD_COLLATERAL_QUALIFIED;  --����ѺƷ�ϸ�ӳ���

  v_cc_qlifd cc_qlifd%rowtype;
  --�����α�,Ȩ�ط��ϸ��������͹���(��ѺƷĿ¼)
  cursor cc_type is
  	select guaranty_kind2, --ѺƷ�������
           std_code        --Ȩ�ط�����ѺƷ���ʹ���
      from RWA_DEV.RWA_CD_COLLATERAL_TYPE;  --����ѺƷ���ʹ����

  v_cc_type cc_type%rowtype;
  --�����α�,Ȩ�ط��ϸ�����ϸ�ֹ���(��ѺƷĿ¼)
  cursor cc_sdvs is
  	select guaranty_kind2, --ѺƷ�������
    collateral_std_detail  --Ȩ�ط�������ϸ��
      from RWA_DEV.RWA_CD_COLLATERAL_STD; --�ϸ����ѺƷȨ�ط�ӳ���

  v_cc_sdvs cc_sdvs%rowtype;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    --1.���µ���ѺƷ��ĺϸ��϶�״̬Ϊ��
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL SET QUALFLAGSTD = '''' WHERE SOURCECOLSUBTYPE IS NOT NULL AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;

    --2.��ȡ��ִ�еĸ���sql
    --2.1 ��ȡ�ϸ��϶�sql
    for v_cc_qlifd in cc_qlifd loop
    	if v_cc_qlifd.qualified_flag <> '01' then
    		v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL SET QUALFLAGSTD = ''0'', COLLATERALTYPESTD = '''', COLLATERALSDVSSTD = '''' WHERE SOURCECOLSUBTYPE = ''' || v_cc_qlifd.guaranty_kind2 || ''' AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    	else
    		v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL SET QUALFLAGSTD = ''1'', COLLATERALTYPESTD = '''', COLLATERALSDVSSTD = '''' WHERE SOURCECOLSUBTYPE = ''' || v_cc_qlifd.guaranty_kind2 || ''' AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    	end if;
    	v_tab_len := v_tab_len + 1;
    end loop;
    --���ʣ�������϶�δ���ϸ���sql
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL SET QUALFLAGSTD = ''0'', COLLATERALTYPESTD = '''', COLLATERALSDVSSTD = '''' WHERE SOURCECOLSUBTYPE IS NOT NULL AND QUALFLAGSTD IS NULL AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;

    --2.2 ��ȡ�Ѻϸ����ѺƷ����
    for v_cc_type in cc_type loop
    	v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL SET COLLATERALTYPESTD = ''' || v_cc_type.std_code || ''' WHERE SOURCECOLSUBTYPE = ''' || v_cc_type.guaranty_kind2 || ''' AND QUALFLAGSTD = ''1'' AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    	v_tab_len := v_tab_len + 1;
    end loop;

    --2.3 ��ȡ�Ѻϸ����ѺƷϸ��
    for v_cc_sdvs in cc_sdvs loop
    	v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL SET COLLATERALSDVSSTD = ''' || v_cc_sdvs.collateral_std_detail || ''' WHERE SOURCECOLSUBTYPE = ''' || v_cc_sdvs.guaranty_kind2 || ''' AND QUALFLAGSTD = ''1'' AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
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
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_COLLATERAL WHERE QUALFLAGSTD = '1' AND SOURCECOLSUBTYPE IS NOT NULL AND DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_COLLATERAL���϶�Ȩ�ط��ºϸ�ĵ���ѺƷ���ݼ�¼Ϊ: ' || v_count || ' ��');


    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := 'Ȩ�ط��ϸ����ѺƷ('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_CD_COLLATERAL_STD;
/

