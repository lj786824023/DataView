CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_COLLATERAL(
                                                              p_data_dt_str    IN    VARCHAR2,        --�������� yyyyMMdd
                                                              p_po_rtncode    OUT    VARCHAR2,        --���ر�� 1 �ɹ�,0 ʧ��
                                                              p_po_rtnmsg     OUT    VARCHAR2         --��������
                                                              )
  /*
    �洢��������:RWA_DEV.PRO_RWA_CD_COLLATERAL
    ʵ�ֹ���:RWAϵͳ-�Ϲ����ݼ�-Ȩ�ط�/�������ϸ������϶�(����ѺƷ)
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
             QHJIANG|2017-04-14|���ݶ�������������Ӧ�ĸ���:����Ȩ�ط����¼����߼����µ������������������ļ����߼�
    */
  Authid Current_User
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_CD_COLLATERAL';
  --������ʱ�ַ�������
  v_clause VARCHAR2(4000) := '';
  --����Ȩ�ط��ϸ��ʶ
  v_qualflagstd VARCHAR2(200) := '';
  --���������������ϸ��ʶ
  v_qualflagfirb VARCHAR2(200) := '';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;
  --����varchar2(4000)����
  type varchartab is table of varchar2(4000) index by binary_integer;
  v_sqlTab varchartab;
  --����������󳤶�
  v_tab_len INTEGER := 1;
  --����sql��¼
  v_err_sql VARCHAR2(4000) := '';
  --�����α�,Ȩ�ط�/�������ϸ������϶�����(��ѺƷĿ¼)
  cursor cc_qlifd is
      select
             --ѺƷ����
             case when guaranty_kind4 is not null
                  then ' AND T.SOURCECOLSUBTYPE = ''' || guaranty_kind4 || ''' '
                  else ' ' end as guaranty_kind4
            --Ȩ�ط�.�Ƿ����з��� ��ֵ:YesOrNo(0-��1-��) 001007001001-��������Ʋ�Ʒ
            ,case when issueflag is not null
                  then ' AND ( (NVL(T.SSYSID,'' '') NOT IN (''LC'',''TZHBJJ'') AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.ISOURS,''02''),''01'',''1'',''0'') = '''||issueflag||'''))
                                   OR (T.SSYSID = ''LC'' AND EXISTS (SELECT 1 FROM RWA_DEV.ZGS_ATINTRUST_PLAN T1 WHERE T.COLLATERALID = T1.C_PRD_CODE AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.C_IS_BANK_ISSUE,''2''),''1'',''1'',''0'') = '''||issueflag||''' ))
                                   OR (T.SSYSID = ''TZHBJJ'' AND EXISTS (SELECT 1 FROM RWA.RWA_WS_BONDTRADE_MF T1 WHERE T.COLLATERALID = T1.MITIGATIONID AND T.DATADATE = T1.DATADATE AND DECODE(NVL(T1.LCISSUERTYPE,''02''),''01'',''1'',''0'') = '''||issueflag||''' ))) '
                  else ' ' end as issueflag
            --Ȩ�ط�.�Ƿ����д浥 ��ֵ:IssueOrg(01-����,02-����) 001001001001-��λ�浥��001001001002-���˴浥
            ,case when selfflag is not null
                  then ' AND ( (NVL(T.SSYSID,'' '') NOT IN (''LC'',''TZHBJJ'') AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.ISOURS,''02'') = '''||selfflag||'''))
                                   OR (T.SSYSID = ''LC''  AND EXISTS (SELECT 1 FROM RWA_DEV.ZGS_ATINTRUST_PLAN T1 WHERE T.COLLATERALID = T1.C_PRD_CODE AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.C_IS_BANK_RECEIPT,''2''),''1'',''01'',''02'') = '''||selfflag||''' ))
                                   OR (T.SSYSID = ''TZHBJJ'' AND EXISTS (SELECT 1 FROM RWA.RWA_WS_BONDTRADE_MF T1 WHERE T.COLLATERALID = T1.MITIGATIONID AND T.DATADATE = T1.DATADATE AND NVL(T1.LETTERTYPE,''02'') = '''||selfflag||''' ))) '
                  else ' ' end as selfflag
            --Ȩ�ط�.ע����ҵ������� ��ֵ:NSERatingGroup
            ,case when country_level_no is not null and country_level_no='01'-- AA-������
                  then ' AND T.RCERATING <= ''0104'' '
                  when country_level_no is not null and country_level_no='02'--AA-�����£�A-��������
                  then ' AND T.RCERating > ''0104'' AND T.RCERATING <= ''0107'' '
                  when country_level_no is not null and country_level_no='03'--A-�����£�BBB-��������
                  then ' AND T.RCERating > ''0107'' AND T.RCERATING <= ''0110'' '
                  when country_level_no is not null and country_level_no='04'--BBB-�����£�B-��������
                  then ' AND T.RCERating > ''0110'' AND T.RCERATING <= ''0116'' '
                  when country_level_no is not null and country_level_no='05'--B-������
                  then ' AND T.RCERATING > ''0116'' '
                  when country_level_no is not null and country_level_no='06'--δ����
                  then ' AND NVL(T.RCERATING,''0124'') IN (''0124'',''0207'') '
                  when country_level_no is not null and country_level_no='07'--A-��������
                  then ' AND T.RCERATING <= ''0107'' '
                  when country_level_no is not null and country_level_no='08'--BBB-��������
                  then ' AND T.RCERATING <= ''0110'' '
                  when country_level_no is not null and country_level_no='09'--B-�������ϣ�A-������
                  then ' AND T.RCERATING <=''0116'' AND T.RCERATING > ''0107'' '
                  else ' ' end as countrylevel
            --Ȩ�ط�.ծȯ����Ŀ�� ��ֵ:BondPublishPurpose(0010-�չ��������ж������е�ծȯ,0020-����)
            ,case when issue_intent_type is not null
                  then ' AND T.SPECPURPBONDFLAG = DECODE('''||issue_intent_type||''',''0010'',''1'',''0'') '
                  else '' end as issue_intent_type
            --������.ծȯ���� ��ֵ:IRBRatingGroup
            ,case when out_judege_type is not null and out_judege_type='01'--01-BBB-��������
                  then ' AND NVL(T.FCISSUERATING,''9999'') <= ''0110'' '
                  when out_judege_type is not null and out_judege_type='02'--02-BBB-������
                  then ' AND NVL(T.FCISSUERATING,''9999'') > ''0110'' '
                  when out_judege_type is not null and out_judege_type='03'--03-BB-��������
                  then ' AND NVL(T.FCISSUERATING,''9999'') <= ''0113'' '
                  when out_judege_type is not null and out_judege_type='04'--04-BB-������
                  then ' AND NVL(T.FCISSUERATING,''9999'') > ''0113'' '
                  when out_judege_type is not null and out_judege_type='05'--05-A-3��������
                  then ' AND NVL(T.FCISSUERATING,''9999'') <= ''0203'' '
                  when out_judege_type is not null and out_judege_type='06'--06-A-3������
                  then ' AND NVL(T.FCISSUERATING,''9999'') > ''0203'' '
                  else ' ' end as out_judege_type
            --������.�����˷��е�ͬһ����ծȯ�ⲿ�����Ƿ�BBB-��A-3/P-3������ ��ֵ:YesOrNo(0-��1-��)
            ,case when isratingupper is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.ISRATINGUPPER,''02''),''01'',''1'',''0'') = '''||isratingupper||''') '
                  else '' end as isratingupper
            --������.Ͷ�ʽ��ڹ������� ��ֵ:YesOrNo(0-��1-��)
            ,case when tooltype is not null and tooltype='1'
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.TOOLTYPE,''14'') <> ''14'') '
                  when tooltype is not null and tooltype='0'
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.TOOLTYPE,''14'') = ''14'') '
                  else '' end tooltype
            --������.�Ƿ��ת�� ��ֵ:YesOrNo(0-��1-��)
            ,case when istransfer is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.ISTRANSFER,''02''),''01'',''1'',''0'') = '''||istransfer||''') '
                  else '' end istransfer
            --������.�Ƿ�ÿ�չ������� ��ֵ:YesOrNo(0-��1-��)
            ,case when ispubliceveryvalue is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.ISPUBLICEVERYVALUE,''02''),''01'',''1'',''0'') = '''||ispubliceveryvalue||''') '
                  else '' end as ispubliceveryvalue
            --������.ȡ�÷�ʽ ��ֵ:IRBLand 002002001001-��ҵ�õ�,002002003001-�칫�õ�,002004001001-��ס�õ�
            ,case when landproperty is not null and landproperty='02'--02 �ǳ���
                  then ' AND ( (NVL(T.SSYSID,'' '') <> ''LC'' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_REALTY T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.LANDPROPERTY,''03'') <> ''01'')) '||
                       '      OR (T.SSYSID=''LC'' AND EXISTS(SELECT 1 FROM RWA_DEV.ZGS_ATINTRUST_PLAN T1 WHERE T.COLLATERALID = T1.C_PRD_CODE AND T.DATANO = T1.DATANO AND NVL(T1.C_ACQUIRE_WAY,'' '') <>  ''1'' ))) '
                  when landproperty is not null and landproperty='01'--01 ����
                  then ' AND ( (NVL(T.SSYSID,'' '') <> ''LC'' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_REALTY T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.LANDPROPERTY,''03'') = ''01'')) '||
                       '      OR (T.SSYSID=''LC'' AND EXISTS(SELECT 1 FROM RWA_DEV.ZGS_ATINTRUST_PLAN T1 WHERE T.COLLATERALID = T1.C_PRD_CODE AND T.DATANO = T1.DATANO AND NVL(T1.C_ACQUIRE_WAY,'' '') = ''1'' ))) '
                  else '' end as landproperty
            --������.���� ��ֵ:IRBAge
            ,case when rcvage is not null and rcvage='01'--01-����>12
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_RECEIVABLE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.RCVAGE,''0'')> ''12'') '
                  when rcvage is not null and rcvage='02' --02-����<=12
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_RECEIVABLE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.RCVAGE,''0'')<= ''12'') '
                  else '' end as rcvage
            ,qualified_flag     --�ϸ��ʶ ��ֵ:QualificationFlag
        from RWA_DEV.RWA_CD_COLLATERAL_QUALIFIED order by serial_no asc;

  v_cc_qlifd cc_qlifd%rowtype;
  --�����α�,Ȩ�ط�/�������ϸ��������͹���(��ѺƷĿ¼)
  cursor cc_type is
      select
             --ѺƷ����
             case when guaranty_kind4 is not null
                  then ' AND T.SOURCECOLSUBTYPE = ''' || guaranty_kind4 || ''' '
                  else ' ' end as guaranty_kind4
            --Ȩ�ط�.�Ƿ����з��� ��ֵ:YesOrNo
            ,case when issueflag is not null
                  then ' AND ( (NVL(T.SSYSID,'' '') NOT IN (''LC'',''TZHBJJ'') AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.ISOURS,''02''),''01'',''1'',''0'') = '''||issueflag||'''))
                                   OR (T.SSYSID = ''LC'' AND EXISTS (SELECT 1 FROM RWA_DEV.ZGS_ATINTRUST_PLAN T1 WHERE T.COLLATERALID = T1.C_PRD_CODE AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.C_IS_BANK_ISSUE,''2''),''1'',''1'',''0'') = '''||issueflag||''' ))
                                   OR (T.SSYSID = ''TZHBJJ'' AND EXISTS (SELECT 1 FROM RWA.RWA_WS_BONDTRADE_MF T1 WHERE T.COLLATERALID = T1.MITIGATIONID AND T.DATADATE = T1.DATADATE AND DECODE(NVL(T1.LCISSUERTYPE,''02''),''01'',''1'',''0'') = '''||issueflag||''' ))) '
                  else ' ' end as issueflag
            --Ȩ�ط�.�Ƿ����д浥 ��ֵ:IssueOrg
            ,case when selfflag is not null
                  then ' AND ( (NVL(T.SSYSID,'' '') NOT IN (''LC'',''TZHBJJ'') AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.ISOURS,''02'') = '''||selfflag||'''))
                                   OR (T.SSYSID = ''LC''  AND EXISTS (SELECT 1 FROM RWA_DEV.ZGS_ATINTRUST_PLAN T1 WHERE T.COLLATERALID = T1.C_PRD_CODE AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.C_IS_BANK_RECEIPT,''2''),''1'',''01'',''02'') = '''||selfflag||''' ))
                                   OR (T.SSYSID = ''TZHBJJ'' AND EXISTS (SELECT 1 FROM RWA.RWA_WS_BONDTRADE_MF T1 WHERE T.COLLATERALID = T1.MITIGATIONID AND T.DATADATE = T1.DATADATE AND NVL(T1.LETTERTYPE,''02'') = '''||selfflag||''' ))) '
                  else ' ' end as selfflag
            --Ȩ�ط�.ע����ҵ������� ��ֵ:NSERatingGroup
            ,case when country_level_no is not null and country_level_no='01'-- AA-������
                  then ' AND T.RCERATING <= ''0104'' '
                  when country_level_no is not null and country_level_no='02'--AA-�����£�A-��������
                  then ' AND T.RCERating > ''0104'' AND T.RCERATING <= ''0107'' '
                  when country_level_no is not null and country_level_no='03'--A-�����£�BBB-��������
                  then ' AND T.RCERating > ''0107'' AND T.RCERATING <= ''0110'' '
                  when country_level_no is not null and country_level_no='04'--BBB-�����£�B-��������
                  then ' AND T.RCERating > ''0110'' AND T.RCERATING <= ''0116'' '
                  when country_level_no is not null and country_level_no='05'--B-������
                  then ' AND T.RCERATING > ''0116'' '
                  when country_level_no is not null and country_level_no='06'--δ����
                  then ' AND NVL(T.RCERATING,''0124'') IN (''0124'',''0207'') '
                  when country_level_no is not null and country_level_no='07'--A-��������
                  then ' AND T.RCERATING <= ''0107'' '
                  when country_level_no is not null and country_level_no='08'--BBB-��������
                  then ' AND T.RCERATING <= ''0110'' '
                  when country_level_no is not null and country_level_no='09'--B-�������ϣ�A-������
                  then ' AND T.RCERATING <=''0116'' AND T.RCERATING > ''0107'' '
                  else ' ' end as countrylevel
            --Ȩ�ط�.ծȯ����Ŀ�� ��ֵ:BondPublishPurpose
            ,case when issue_intent_type is not null
                  then ' AND T.SPECPURPBONDFLAG = DECODE('''||issue_intent_type||''',''0010'',''1'',''0'') '
                  else '' end as issue_intent_type
            --������.ծȯ���� ��ֵ:IRBRatingGroup
            ,case when out_judege_type is not null and out_judege_type='01'--01-BBB-��������
                  then ' AND NVL(T.FCISSUERATING,''9999'') <= ''0110'' '
                  when out_judege_type is not null and out_judege_type='02'--02-BBB-������
                  then ' AND NVL(T.FCISSUERATING,''9999'') > ''0110'' '
                  when out_judege_type is not null and out_judege_type='03'--03-BB-��������
                  then ' AND NVL(T.FCISSUERATING,''9999'') <= ''0113'' '
                  when out_judege_type is not null and out_judege_type='04'--04-BB-������
                  then ' AND NVL(T.FCISSUERATING,''9999'') > ''0113'' '
                  when out_judege_type is not null and out_judege_type='05'--05-A-3��������
                  then ' AND NVL(T.FCISSUERATING,''9999'') <= ''0203'' '
                  when out_judege_type is not null and out_judege_type='06'--06-A-3������
                  then ' AND NVL(T.FCISSUERATING,''9999'') > ''0203'' '
                  else ' ' end as out_judege_type
            --������.�����˷��е�ͬһ����ծȯ�ⲿ�����Ƿ�BBB-��A-3/P-3������ ��ֵ:YesOrNo
            ,case when isratingupper is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.ISRATINGUPPER,''02''),''01'',''1'',''0'') = '''||isratingupper||''') '
                  else '' end as isratingupper
            --������.Ͷ�ʽ��ڹ������� ��ֵ:YesOrNo
            ,case when tooltype is not null and tooltype='1'
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.TOOLTYPE,''14'') <> ''14'') '
                  when tooltype is not null and tooltype='0'
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.TOOLTYPE,''14'') = ''14'') '
                  else '' end tooltype
            --������.�Ƿ��ת�� ��ֵ:YesOrNo
            ,case when istransfer is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.ISTRANSFER,''02''),''01'',''1'',''0'') = '''||istransfer||''') '
                  else '' end istransfer
            --������.�Ƿ�ÿ�չ������� ��ֵ:YesOrNo
            ,case when ispubliceveryvalue is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.ISPUBLICEVERYVALUE,''02''),''01'',''1'',''0'') = '''||ispubliceveryvalue||''') '
                  else '' end as ispubliceveryvalue
            --������.ȡ�÷�ʽ ��ֵ:IRBLand
            ,case when landproperty is not null and landproperty='02'--02 �ǳ���
                  then ' AND ( (NVL(T.SSYSID,'' '') <> ''LC'' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_REALTY T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.LANDPROPERTY,''03'') <> ''01'')) '||
                       '      OR (T.SSYSID=''LC'' AND EXISTS(SELECT 1 FROM RWA_DEV.ZGS_ATINTRUST_PLAN T1 WHERE T.COLLATERALID = T1.C_PRD_CODE AND T.DATANO = T1.DATANO AND NVL(T1.C_ACQUIRE_WAY,'' '') <>  ''1'' ))) '
                  when landproperty is not null and landproperty='01'--01 ����
                  then ' AND ( (NVL(T.SSYSID,'' '') <> ''LC'' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_REALTY T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.LANDPROPERTY,''03'') = ''01'')) '||
                       '      OR (T.SSYSID=''LC'' AND EXISTS(SELECT 1 FROM RWA_DEV.ZGS_ATINTRUST_PLAN T1 WHERE T.COLLATERALID = T1.C_PRD_CODE AND T.DATANO = T1.DATANO AND NVL(T1.C_ACQUIRE_WAY,'' '') = ''1'' ))) '
                  else '' end as landproperty
            --������.���� ��ֵ:IRBAge
            ,case when rcvage is not null and rcvage='01'--01-����>12
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_RECEIVABLE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.RCVAGE,0) > 12) '
                  when rcvage is not null and rcvage='02' --02-����<=12
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_RECEIVABLE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.RCVAGE,0) <= 12) '
                  else '' end as rcvage
            --Ȩ�ط�����ѺƷ���ʹ���
            ,std_code
            --����������ѺƷ���ʹ���
            ,irb_code
            --������������ѺƷ����
            ,irb_financial_code
        from RWA_DEV.RWA_CD_COLLATERAL_TYPE  order by serial_no asc;

  v_cc_type cc_type%rowtype;
  --�����α�,Ȩ�ط�/�������ϸ�����ϸ�ֹ���(��ѺƷĿ¼)
  cursor cc_sdvs is
      select --ѺƷ����
             case when guaranty_kind4 is not null
                  then ' AND T.SOURCECOLSUBTYPE = ''' || guaranty_kind4 || ''' '
                  else ' ' end as guaranty_kind4
            --Ȩ�ط�.�Ƿ����з��� ��ֵ:YesOrNo
            ,case when issueflag is not null
                  then ' AND ( (NVL(T.SSYSID,'' '') NOT IN (''LC'',''TZHBJJ'') AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.ISOURS,''02''),''01'',''1'',''0'') = '''||issueflag||'''))
                                   OR (T.SSYSID = ''LC'' AND EXISTS (SELECT 1 FROM RWA_DEV.ZGS_ATINTRUST_PLAN T1 WHERE T.COLLATERALID = T1.C_PRD_CODE AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.C_IS_BANK_ISSUE,''2''),''1'',''1'',''0'') = '''||issueflag||''' ))
                                   OR (T.SSYSID = ''TZHBJJ'' AND EXISTS (SELECT 1 FROM RWA.RWA_WS_BONDTRADE_MF T1 WHERE T.COLLATERALID = T1.MITIGATIONID AND T.DATADATE = T1.DATADATE AND DECODE(NVL(T1.LCISSUERTYPE,''02''),''01'',''1'',''0'') = '''||issueflag||''' ))) '
                  else ' ' end as issueflag
            --Ȩ�ط�.�Ƿ����д浥 ��ֵ:IssueOrg
            ,case when selfflag is not null
                  then ' AND ( (NVL(T.SSYSID,'' '') NOT IN (''LC'',''TZHBJJ'') AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.ISOURS,''02'') = '''||selfflag||'''))
                                   OR (T.SSYSID = ''LC''  AND EXISTS (SELECT 1 FROM RWA_DEV.ZGS_ATINTRUST_PLAN T1 WHERE T.COLLATERALID = T1.C_PRD_CODE AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.C_IS_BANK_RECEIPT,''2''),''1'',''01'',''02'') = '''||selfflag||''' ))
                                   OR (T.SSYSID = ''TZHBJJ'' AND EXISTS (SELECT 1 FROM RWA.RWA_WS_BONDTRADE_MF T1 WHERE T.COLLATERALID = T1.MITIGATIONID AND T.DATADATE = T1.DATADATE AND NVL(T1.LETTERTYPE,''02'') = '''||selfflag||''' ))) '
                  else ' ' end as selfflag
            --Ȩ�ط�.ע����ҵ������� ��ֵ:NSERatingGroup
            ,case when country_level_no is not null and country_level_no='01'-- AA-������
                  then ' AND T.RCERATING <= ''0104'' '
                  when country_level_no is not null and country_level_no='02'--AA-�����£�A-��������
                  then ' AND T.RCERating > ''0104'' AND T.RCERATING <= ''0107'' '
                  when country_level_no is not null and country_level_no='03'--A-�����£�BBB-��������
                  then ' AND T.RCERating > ''0107'' AND T.RCERATING <= ''0110'' '
                  when country_level_no is not null and country_level_no='04'--BBB-�����£�B-��������
                  then ' AND T.RCERating > ''0110'' AND T.RCERATING <= ''0116'' '
                  when country_level_no is not null and country_level_no='05'--B-������
                  then ' AND T.RCERATING > ''0116'' '
                  when country_level_no is not null and country_level_no='06'--δ����
                  then ' AND NVL(T.RCERATING,''0124'') IN (''0124'',''0207'') '
                  when country_level_no is not null and country_level_no='07'--A-��������
                  then ' AND T.RCERATING <= ''0107'' '
                  when country_level_no is not null and country_level_no='08'--BBB-��������
                  then ' AND T.RCERATING <= ''0110'' '
                  when country_level_no is not null and country_level_no='09'--B-�������ϣ�A-������
                  then ' AND T.RCERATING <=''0116'' AND T.RCERATING > ''0107'' '
                  else ' ' end as countrylevel
            --Ȩ�ط�.ծȯ����Ŀ�� ��ֵ:BondPublishPurpose
            ,case when issue_intent_type is not null
                  then ' AND T.SPECPURPBONDFLAG = DECODE('''||issue_intent_type||''',''0010'',''1'',''0'') '
                  else '' end as issue_intent_type
            ,collateral_std_detail
        from RWA_DEV.RWA_CD_COLLATERAL_STD  order by serial_no asc;

  v_cc_sdvs cc_sdvs%rowtype;

  BEGIN
    dbms_output.enable(20000);
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    --1.���µ���ѺƷ��ĺϸ��϶�״̬Ϊ��
    --v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL SET QUALFLAGSTD = '''',COLLATERALTYPESTD='''',COLLATERALSDVSSTD='''',QUALFLAGFIRB = '''',COLLATERALTYPEIRB='''',FCTYPE='''' WHERE DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'') AND SSYSID <> ''HG''';
    --������Ȩ�ط����֣���������ԭϵͳ��ȡ��RWAϵͳ������
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL SET QUALFLAGSTD = '''',COLLATERALTYPESTD='''',COLLATERALSDVSSTD='''' WHERE DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'') AND SSYSID <> ''HG''';
    v_tab_len := v_tab_len + 1;

    --2.��ȡ��ִ�еĸ���sql
    --2.1 ��ȡ�ϸ��϶�sql
    for v_cc_qlifd in cc_qlifd loop
        v_qualflagstd := '';
        v_qualflagfirb := '';
        v_clause :=  v_cc_qlifd.guaranty_kind4     --ѺƷ����
                   ||v_cc_qlifd.issueflag          --Ȩ�ط�.�Ƿ����з��� ��ֵ:YesOrNo
                   ||v_cc_qlifd.selfflag           --Ȩ�ط�.�Ƿ����д浥 ��ֵ:IssueOrg
                   ||v_cc_qlifd.countrylevel       --Ȩ�ط�.ע����ҵ������� ��ֵ:NSERatingGroup
                   ||v_cc_qlifd.issue_intent_type  --Ȩ�ط�.ծȯ����Ŀ�� ��ֵ:BondPublishPurpose
                   ||v_cc_qlifd.out_judege_type    --������.ծȯ���� ��ֵ:ERating
                   ||v_cc_qlifd.isratingupper      --������.�����˷��е�ͬһ����ծȯ�ⲿ�����Ƿ�BBB-��A-3/P-3������ ��ֵ:YesOrNo
                   ||v_cc_qlifd.tooltype           --������.Ͷ�ʽ��ڹ������� ��ֵ:YesOrNo
                   ||v_cc_qlifd.istransfer         --������.�Ƿ��ת�� ��ֵ:YesOrNo
                   ||v_cc_qlifd.ispubliceveryvalue --������.�Ƿ�ÿ�չ������� ��ֵ:YesOrNo
                   ||v_cc_qlifd.landproperty       --������.ȡ�÷�ʽ ��ֵ:cms_code_library.CmsLandProperty01
                   ||v_cc_qlifd.rcvage;            --������.����
        if v_cc_qlifd.qualified_flag = '01' then     --T Ȩ�ط����������ϸ� ����:QualificationFlag
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
            v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL T
                                       SET  T.QUALFLAGSTD = NVL(T.QUALFLAGSTD,'''|| v_qualflagstd ||''')
                                           ,T.QUALFLAGFIRB = NVL(T.QUALFLAGFIRB,'''|| v_qualflagfirb ||''')
                                           ,T.COLLATERALTYPESTD = ''''
                                           ,T.COLLATERALSDVSSTD = ''''
                                     WHERE T.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'') AND T.SSYSID <> ''HG'' '||v_clause;
        v_tab_len := v_tab_len + 1;
    end loop;
    --���ʣ�������϶�δ���ϸ���sql
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL T SET T.QUALFLAGSTD = ''0'',T.COLLATERALTYPESTD = '''',T.COLLATERALSDVSSTD = '''' WHERE T.QUALFLAGSTD IS NULL  AND T.SSYSID <> ''HG''AND T.DATADATE = TO_DATE('''|| p_data_dt_str ||''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL T SET T.QUALFLAGFIRB = ''0'',T.COLLATERALTYPEIRB = '''',T.FCTYPE = '''' WHERE T.QUALFLAGFIRB IS NULL  AND T.SSYSID <> ''HG''AND T.DATADATE = TO_DATE('''|| p_data_dt_str ||''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;

    --2.2 ��ȡ�Ѻϸ����ѺƷ����
    for v_cc_type in cc_type loop
        v_clause :=  v_cc_type.guaranty_kind4     --ѺƷ����
                   ||v_cc_type.issueflag          --Ȩ�ط�.�Ƿ����з��� ��ֵ:YesOrNo
                   ||v_cc_type.selfflag           --Ȩ�ط�.�Ƿ����д浥 ��ֵ:IssueOrg
                   ||v_cc_type.countrylevel       --Ȩ�ط�.ע����ҵ������� ��ֵ:NSERatingGroup
                   ||v_cc_type.issue_intent_type  --Ȩ�ط�.ծȯ����Ŀ�� ��ֵ:BondPublishPurpose
                   ||v_cc_type.out_judege_type    --������.ծȯ���� ��ֵ:ERating
                   ||v_cc_type.isratingupper      --������.�����˷��е�ͬһ����ծȯ�ⲿ�����Ƿ�BBB-��A-3/P-3������ ��ֵ:YesOrNo
                   ||v_cc_type.tooltype           --������.Ͷ�ʽ��ڹ������� ��ֵ:YesOrNo
                   ||v_cc_type.istransfer         --������.�Ƿ��ת�� ��ֵ:YesOrNo
                   ||v_cc_type.ispubliceveryvalue --������.�Ƿ�ÿ�չ������� ��ֵ:YesOrNo
                   ||v_cc_type.landproperty       --������.ȡ�÷�ʽ ��ֵ:cms_code_library.CmsLandProperty01
                   ||v_cc_type.rcvage;            --������.����
        v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL T
                                   SET  T.COLLATERALTYPESTD = ''' || v_cc_type.std_code || '''
                                       ,T.COLLATERALSDVSSTD = ''''
                                 WHERE T.QUALFLAGSTD = ''1''
                                   AND T.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')  AND T.SSYSID <> ''HG'''||v_clause;
        v_tab_len := v_tab_len + 1;
        --AND T.COLLATERALTYPEIRB IS NULL
        v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL T
                                   SET  T.COLLATERALTYPEIRB = NVL(T.COLLATERALTYPEIRB,''' || v_cc_type.irb_code || ''')
                                       ,T.FCTYPE = NVL(T.FCTYPE,''' || v_cc_type.irb_financial_code || ''')
                                 WHERE T.QUALFLAGFIRB = ''1''
                                   AND T.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')  AND T.SSYSID <> ''HG'''||v_clause;
        v_tab_len := v_tab_len + 1;
    end loop;

    --2.3 ��ȡ�Ѻϸ����ѺƷϸ��
    for v_cc_sdvs in cc_sdvs loop
        v_clause :=  v_cc_sdvs.guaranty_kind4     --ѺƷ����
                   ||v_cc_sdvs.issueflag          --Ȩ�ط�.�Ƿ����з��� ��ֵ:YesOrNo
                   ||v_cc_sdvs.selfflag           --Ȩ�ط�.�Ƿ����д浥 ��ֵ:IssueOrg
                   ||v_cc_sdvs.countrylevel       --Ȩ�ط�.ע����ҵ������� ��ֵ:NSERatingGroup
                   ||v_cc_sdvs.issue_intent_type; --Ȩ�ط�.ծȯ����Ŀ�� ��ֵ:BondPublishPurpose
        v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL T
                                   SET T.COLLATERALSDVSSTD = ''' || v_cc_sdvs.collateral_std_detail || '''
                                 WHERE T.QUALFLAGSTD = ''1''
                                   AND T.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'') AND T.SSYSID <> ''HG'' '||v_clause;
        v_tab_len := v_tab_len + 1;
    end loop;

    --3.ִ�и���sql
    --delete from qhjiang_test;
    for i in 1..v_sqlTab.count loop
        if v_sqlTab(i) IS NOT NULL THEN
            --insert into qhjiang_test(num1,str) values(i,v_sqlTab(i));
            --commit;
            --Dbms_output.Put_line(v_sqlTab(i));
            v_err_sql := v_sqlTab(i);
            EXECUTE IMMEDIATE v_sqlTab(i);

            COMMIT;
        end if;
    end loop;

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_COLLATERAL WHERE (QUALFLAGSTD IS NOT NULL OR QUALFLAGFIRB IS NOT NULL) AND DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_COLLATERAL���϶�Ȩ�ط�/�������ºϸ�ĵ���ѺƷ���ݼ�¼Ϊ: ' || v_count || ' ��');


    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
            --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
            Dbms_output.Put_line(v_err_sql);
            ROLLBACK;
            p_po_rtncode := sqlcode;
            p_po_rtnmsg  := 'Ȩ�ط�/�������ϸ����ѺƷ('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_CD_COLLATERAL;
/

