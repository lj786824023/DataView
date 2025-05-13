CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_COLLATERAL(
                                                              p_data_dt_str    IN    VARCHAR2,        --数据日期 yyyyMMdd
                                                              p_po_rtncode    OUT    VARCHAR2,        --返回编号 1 成功,0 失败
                                                              p_po_rtnmsg     OUT    VARCHAR2         --返回描述
                                                              )
  /*
    存储过程名称:RWA_DEV.PRO_RWA_CD_COLLATERAL
    实现功能:RWA系统-合规数据集-权重法/内评法合格缓释物认定(抵质押品)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-04-21
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.RWA_EI_COLLATERAL|抵质押品表
    源  表2 :RWA_DEV.RWA_CD_COLLATERAL_QUALIFIED|抵质押品合格映射表
    源  表3 :RWA_DEV.RWA_CD_COLLATERAL_TYPE|抵质押品类型代码表
    源  表4 :RWA_DEV.RWA_CD_COLLATERAL_STD|合格抵质押品权重法映射表
    目标表  :RWA_DEV.RWA_EI_COLLATERAL|抵质押品表
    变更记录(修改人|修改时间|修改内容):
             QHJIANG|2017-04-14|根据二期新需求做相应的改造:二期权重法重新计算逻辑重新调整，并加入内评法的计算逻辑
    */
  Authid Current_User
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_CD_COLLATERAL';
  --定义临时字符串参数
  v_clause VARCHAR2(4000) := '';
  --定义权重法合格标识
  v_qualflagstd VARCHAR2(200) := '';
  --定义内评初级法合格标识
  v_qualflagfirb VARCHAR2(200) := '';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;
  --定义varchar2(4000)数组
  type varchartab is table of varchar2(4000) index by binary_integer;
  v_sqlTab varchartab;
  --定义数组最大长度
  v_tab_len INTEGER := 1;
  --错误sql记录
  v_err_sql VARCHAR2(4000) := '';
  --定义游标,权重法/内评法合格缓释物认定规则(新押品目录)
  cursor cc_qlifd is
      select
             --押品四类
             case when guaranty_kind4 is not null
                  then ' AND T.SOURCECOLSUBTYPE = ''' || guaranty_kind4 || ''' '
                  else ' ' end as guaranty_kind4
            --权重法.是否我行发行 码值:YesOrNo(0-否，1-是) 001007001001-保本型理财产品
            ,case when issueflag is not null
                  then ' AND ( (NVL(T.SSYSID,'' '') NOT IN (''LC'',''TZHBJJ'') AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.ISOURS,''02''),''01'',''1'',''0'') = '''||issueflag||'''))
                                   OR (T.SSYSID = ''LC'' AND EXISTS (SELECT 1 FROM RWA_DEV.ZGS_ATINTRUST_PLAN T1 WHERE T.COLLATERALID = T1.C_PRD_CODE AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.C_IS_BANK_ISSUE,''2''),''1'',''1'',''0'') = '''||issueflag||''' ))
                                   OR (T.SSYSID = ''TZHBJJ'' AND EXISTS (SELECT 1 FROM RWA.RWA_WS_BONDTRADE_MF T1 WHERE T.COLLATERALID = T1.MITIGATIONID AND T.DATADATE = T1.DATADATE AND DECODE(NVL(T1.LCISSUERTYPE,''02''),''01'',''1'',''0'') = '''||issueflag||''' ))) '
                  else ' ' end as issueflag
            --权重法.是否我行存单 码值:IssueOrg(01-本行,02-他行) 001001001001-单位存单，001001001002-个人存单
            ,case when selfflag is not null
                  then ' AND ( (NVL(T.SSYSID,'' '') NOT IN (''LC'',''TZHBJJ'') AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.ISOURS,''02'') = '''||selfflag||'''))
                                   OR (T.SSYSID = ''LC''  AND EXISTS (SELECT 1 FROM RWA_DEV.ZGS_ATINTRUST_PLAN T1 WHERE T.COLLATERALID = T1.C_PRD_CODE AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.C_IS_BANK_RECEIPT,''2''),''1'',''01'',''02'') = '''||selfflag||''' ))
                                   OR (T.SSYSID = ''TZHBJJ'' AND EXISTS (SELECT 1 FROM RWA.RWA_WS_BONDTRADE_MF T1 WHERE T.COLLATERALID = T1.MITIGATIONID AND T.DATADATE = T1.DATADATE AND NVL(T1.LETTERTYPE,''02'') = '''||selfflag||''' ))) '
                  else ' ' end as selfflag
            --权重法.注册国家地区评级 码值:NSERatingGroup
            ,case when country_level_no is not null and country_level_no='01'-- AA-级以上
                  then ' AND T.RCERATING <= ''0104'' '
                  when country_level_no is not null and country_level_no='02'--AA-级以下，A-级及以上
                  then ' AND T.RCERating > ''0104'' AND T.RCERATING <= ''0107'' '
                  when country_level_no is not null and country_level_no='03'--A-级以下，BBB-级及以上
                  then ' AND T.RCERating > ''0107'' AND T.RCERATING <= ''0110'' '
                  when country_level_no is not null and country_level_no='04'--BBB-级以下，B-级及以上
                  then ' AND T.RCERating > ''0110'' AND T.RCERATING <= ''0116'' '
                  when country_level_no is not null and country_level_no='05'--B-级以下
                  then ' AND T.RCERATING > ''0116'' '
                  when country_level_no is not null and country_level_no='06'--未评级
                  then ' AND NVL(T.RCERATING,''0124'') IN (''0124'',''0207'') '
                  when country_level_no is not null and country_level_no='07'--A-级及以上
                  then ' AND T.RCERATING <= ''0107'' '
                  when country_level_no is not null and country_level_no='08'--BBB-级及以上
                  then ' AND T.RCERATING <= ''0110'' '
                  when country_level_no is not null and country_level_no='09'--B-级及以上，A-级以下
                  then ' AND T.RCERATING <=''0116'' AND T.RCERATING > ''0107'' '
                  else ' ' end as countrylevel
            --权重法.债券发行目的 码值:BondPublishPurpose(0010-收购国有银行而定向发行的债券,0020-其他)
            ,case when issue_intent_type is not null
                  then ' AND T.SPECPURPBONDFLAG = DECODE('''||issue_intent_type||''',''0010'',''1'',''0'') '
                  else '' end as issue_intent_type
            --内评法.债券评级 码值:IRBRatingGroup
            ,case when out_judege_type is not null and out_judege_type='01'--01-BBB-级及以上
                  then ' AND NVL(T.FCISSUERATING,''9999'') <= ''0110'' '
                  when out_judege_type is not null and out_judege_type='02'--02-BBB-级以下
                  then ' AND NVL(T.FCISSUERATING,''9999'') > ''0110'' '
                  when out_judege_type is not null and out_judege_type='03'--03-BB-级及以上
                  then ' AND NVL(T.FCISSUERATING,''9999'') <= ''0113'' '
                  when out_judege_type is not null and out_judege_type='04'--04-BB-级以下
                  then ' AND NVL(T.FCISSUERATING,''9999'') > ''0113'' '
                  when out_judege_type is not null and out_judege_type='05'--05-A-3级及以上
                  then ' AND NVL(T.FCISSUERATING,''9999'') <= ''0203'' '
                  when out_judege_type is not null and out_judege_type='06'--06-A-3级以下
                  then ' AND NVL(T.FCISSUERATING,''9999'') > ''0203'' '
                  else ' ' end as out_judege_type
            --内评法.发行人发行的同一级别债券外部评级是否BBB-或A-3/P-3及以上 码值:YesOrNo(0-否，1-是)
            ,case when isratingupper is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.ISRATINGUPPER,''02''),''01'',''1'',''0'') = '''||isratingupper||''') '
                  else '' end as isratingupper
            --内评法.投资金融工具类型 码值:YesOrNo(0-否，1-是)
            ,case when tooltype is not null and tooltype='1'
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.TOOLTYPE,''14'') <> ''14'') '
                  when tooltype is not null and tooltype='0'
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.TOOLTYPE,''14'') = ''14'') '
                  else '' end tooltype
            --内评法.是否可转让 码值:YesOrNo(0-否，1-是)
            ,case when istransfer is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.ISTRANSFER,''02''),''01'',''1'',''0'') = '''||istransfer||''') '
                  else '' end istransfer
            --内评法.是否每日公开报价 码值:YesOrNo(0-否，1-是)
            ,case when ispubliceveryvalue is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.ISPUBLICEVERYVALUE,''02''),''01'',''1'',''0'') = '''||ispubliceveryvalue||''') '
                  else '' end as ispubliceveryvalue
            --内评法.取得方式 码值:IRBLand 002002001001-商业用地,002002003001-办公用地,002004001001-居住用地
            ,case when landproperty is not null and landproperty='02'--02 非出让
                  then ' AND ( (NVL(T.SSYSID,'' '') <> ''LC'' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_REALTY T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.LANDPROPERTY,''03'') <> ''01'')) '||
                       '      OR (T.SSYSID=''LC'' AND EXISTS(SELECT 1 FROM RWA_DEV.ZGS_ATINTRUST_PLAN T1 WHERE T.COLLATERALID = T1.C_PRD_CODE AND T.DATANO = T1.DATANO AND NVL(T1.C_ACQUIRE_WAY,'' '') <>  ''1'' ))) '
                  when landproperty is not null and landproperty='01'--01 出让
                  then ' AND ( (NVL(T.SSYSID,'' '') <> ''LC'' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_REALTY T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.LANDPROPERTY,''03'') = ''01'')) '||
                       '      OR (T.SSYSID=''LC'' AND EXISTS(SELECT 1 FROM RWA_DEV.ZGS_ATINTRUST_PLAN T1 WHERE T.COLLATERALID = T1.C_PRD_CODE AND T.DATANO = T1.DATANO AND NVL(T1.C_ACQUIRE_WAY,'' '') = ''1'' ))) '
                  else '' end as landproperty
            --内评法.账龄 码值:IRBAge
            ,case when rcvage is not null and rcvage='01'--01-账龄>12
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_RECEIVABLE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.RCVAGE,''0'')> ''12'') '
                  when rcvage is not null and rcvage='02' --02-账龄<=12
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_RECEIVABLE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.RCVAGE,''0'')<= ''12'') '
                  else '' end as rcvage
            ,qualified_flag     --合格标识 码值:QualificationFlag
        from RWA_DEV.RWA_CD_COLLATERAL_QUALIFIED order by serial_no asc;

  v_cc_qlifd cc_qlifd%rowtype;
  --定义游标,权重法/内评法合格缓释物类型规则(新押品目录)
  cursor cc_type is
      select
             --押品四类
             case when guaranty_kind4 is not null
                  then ' AND T.SOURCECOLSUBTYPE = ''' || guaranty_kind4 || ''' '
                  else ' ' end as guaranty_kind4
            --权重法.是否我行发行 码值:YesOrNo
            ,case when issueflag is not null
                  then ' AND ( (NVL(T.SSYSID,'' '') NOT IN (''LC'',''TZHBJJ'') AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.ISOURS,''02''),''01'',''1'',''0'') = '''||issueflag||'''))
                                   OR (T.SSYSID = ''LC'' AND EXISTS (SELECT 1 FROM RWA_DEV.ZGS_ATINTRUST_PLAN T1 WHERE T.COLLATERALID = T1.C_PRD_CODE AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.C_IS_BANK_ISSUE,''2''),''1'',''1'',''0'') = '''||issueflag||''' ))
                                   OR (T.SSYSID = ''TZHBJJ'' AND EXISTS (SELECT 1 FROM RWA.RWA_WS_BONDTRADE_MF T1 WHERE T.COLLATERALID = T1.MITIGATIONID AND T.DATADATE = T1.DATADATE AND DECODE(NVL(T1.LCISSUERTYPE,''02''),''01'',''1'',''0'') = '''||issueflag||''' ))) '
                  else ' ' end as issueflag
            --权重法.是否我行存单 码值:IssueOrg
            ,case when selfflag is not null
                  then ' AND ( (NVL(T.SSYSID,'' '') NOT IN (''LC'',''TZHBJJ'') AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.ISOURS,''02'') = '''||selfflag||'''))
                                   OR (T.SSYSID = ''LC''  AND EXISTS (SELECT 1 FROM RWA_DEV.ZGS_ATINTRUST_PLAN T1 WHERE T.COLLATERALID = T1.C_PRD_CODE AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.C_IS_BANK_RECEIPT,''2''),''1'',''01'',''02'') = '''||selfflag||''' ))
                                   OR (T.SSYSID = ''TZHBJJ'' AND EXISTS (SELECT 1 FROM RWA.RWA_WS_BONDTRADE_MF T1 WHERE T.COLLATERALID = T1.MITIGATIONID AND T.DATADATE = T1.DATADATE AND NVL(T1.LETTERTYPE,''02'') = '''||selfflag||''' ))) '
                  else ' ' end as selfflag
            --权重法.注册国家地区评级 码值:NSERatingGroup
            ,case when country_level_no is not null and country_level_no='01'-- AA-级以上
                  then ' AND T.RCERATING <= ''0104'' '
                  when country_level_no is not null and country_level_no='02'--AA-级以下，A-级及以上
                  then ' AND T.RCERating > ''0104'' AND T.RCERATING <= ''0107'' '
                  when country_level_no is not null and country_level_no='03'--A-级以下，BBB-级及以上
                  then ' AND T.RCERating > ''0107'' AND T.RCERATING <= ''0110'' '
                  when country_level_no is not null and country_level_no='04'--BBB-级以下，B-级及以上
                  then ' AND T.RCERating > ''0110'' AND T.RCERATING <= ''0116'' '
                  when country_level_no is not null and country_level_no='05'--B-级以下
                  then ' AND T.RCERATING > ''0116'' '
                  when country_level_no is not null and country_level_no='06'--未评级
                  then ' AND NVL(T.RCERATING,''0124'') IN (''0124'',''0207'') '
                  when country_level_no is not null and country_level_no='07'--A-级及以上
                  then ' AND T.RCERATING <= ''0107'' '
                  when country_level_no is not null and country_level_no='08'--BBB-级及以上
                  then ' AND T.RCERATING <= ''0110'' '
                  when country_level_no is not null and country_level_no='09'--B-级及以上，A-级以下
                  then ' AND T.RCERATING <=''0116'' AND T.RCERATING > ''0107'' '
                  else ' ' end as countrylevel
            --权重法.债券发行目的 码值:BondPublishPurpose
            ,case when issue_intent_type is not null
                  then ' AND T.SPECPURPBONDFLAG = DECODE('''||issue_intent_type||''',''0010'',''1'',''0'') '
                  else '' end as issue_intent_type
            --内评法.债券评级 码值:IRBRatingGroup
            ,case when out_judege_type is not null and out_judege_type='01'--01-BBB-级及以上
                  then ' AND NVL(T.FCISSUERATING,''9999'') <= ''0110'' '
                  when out_judege_type is not null and out_judege_type='02'--02-BBB-级以下
                  then ' AND NVL(T.FCISSUERATING,''9999'') > ''0110'' '
                  when out_judege_type is not null and out_judege_type='03'--03-BB-级及以上
                  then ' AND NVL(T.FCISSUERATING,''9999'') <= ''0113'' '
                  when out_judege_type is not null and out_judege_type='04'--04-BB-级以下
                  then ' AND NVL(T.FCISSUERATING,''9999'') > ''0113'' '
                  when out_judege_type is not null and out_judege_type='05'--05-A-3级及以上
                  then ' AND NVL(T.FCISSUERATING,''9999'') <= ''0203'' '
                  when out_judege_type is not null and out_judege_type='06'--06-A-3级以下
                  then ' AND NVL(T.FCISSUERATING,''9999'') > ''0203'' '
                  else ' ' end as out_judege_type
            --内评法.发行人发行的同一级别债券外部评级是否BBB-或A-3/P-3及以上 码值:YesOrNo
            ,case when isratingupper is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.ISRATINGUPPER,''02''),''01'',''1'',''0'') = '''||isratingupper||''') '
                  else '' end as isratingupper
            --内评法.投资金融工具类型 码值:YesOrNo
            ,case when tooltype is not null and tooltype='1'
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.TOOLTYPE,''14'') <> ''14'') '
                  when tooltype is not null and tooltype='0'
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.TOOLTYPE,''14'') = ''14'') '
                  else '' end tooltype
            --内评法.是否可转让 码值:YesOrNo
            ,case when istransfer is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.ISTRANSFER,''02''),''01'',''1'',''0'') = '''||istransfer||''') '
                  else '' end istransfer
            --内评法.是否每日公开报价 码值:YesOrNo
            ,case when ispubliceveryvalue is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.ISPUBLICEVERYVALUE,''02''),''01'',''1'',''0'') = '''||ispubliceveryvalue||''') '
                  else '' end as ispubliceveryvalue
            --内评法.取得方式 码值:IRBLand
            ,case when landproperty is not null and landproperty='02'--02 非出让
                  then ' AND ( (NVL(T.SSYSID,'' '') <> ''LC'' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_REALTY T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.LANDPROPERTY,''03'') <> ''01'')) '||
                       '      OR (T.SSYSID=''LC'' AND EXISTS(SELECT 1 FROM RWA_DEV.ZGS_ATINTRUST_PLAN T1 WHERE T.COLLATERALID = T1.C_PRD_CODE AND T.DATANO = T1.DATANO AND NVL(T1.C_ACQUIRE_WAY,'' '') <>  ''1'' ))) '
                  when landproperty is not null and landproperty='01'--01 出让
                  then ' AND ( (NVL(T.SSYSID,'' '') <> ''LC'' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_REALTY T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.LANDPROPERTY,''03'') = ''01'')) '||
                       '      OR (T.SSYSID=''LC'' AND EXISTS(SELECT 1 FROM RWA_DEV.ZGS_ATINTRUST_PLAN T1 WHERE T.COLLATERALID = T1.C_PRD_CODE AND T.DATANO = T1.DATANO AND NVL(T1.C_ACQUIRE_WAY,'' '') = ''1'' ))) '
                  else '' end as landproperty
            --内评法.账龄 码值:IRBAge
            ,case when rcvage is not null and rcvage='01'--01-账龄>12
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_RECEIVABLE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.RCVAGE,0) > 12) '
                  when rcvage is not null and rcvage='02' --02-账龄<=12
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_RECEIVABLE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.RCVAGE,0) <= 12) '
                  else '' end as rcvage
            --权重法抵质押品类型代码
            ,std_code
            --内评法抵质押品类型代码
            ,irb_code
            --内评法金融质押品代码
            ,irb_financial_code
        from RWA_DEV.RWA_CD_COLLATERAL_TYPE  order by serial_no asc;

  v_cc_type cc_type%rowtype;
  --定义游标,权重法/内评法合格缓释物细分规则(新押品目录)
  cursor cc_sdvs is
      select --押品四类
             case when guaranty_kind4 is not null
                  then ' AND T.SOURCECOLSUBTYPE = ''' || guaranty_kind4 || ''' '
                  else ' ' end as guaranty_kind4
            --权重法.是否我行发行 码值:YesOrNo
            ,case when issueflag is not null
                  then ' AND ( (NVL(T.SSYSID,'' '') NOT IN (''LC'',''TZHBJJ'') AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.ISOURS,''02''),''01'',''1'',''0'') = '''||issueflag||'''))
                                   OR (T.SSYSID = ''LC'' AND EXISTS (SELECT 1 FROM RWA_DEV.ZGS_ATINTRUST_PLAN T1 WHERE T.COLLATERALID = T1.C_PRD_CODE AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.C_IS_BANK_ISSUE,''2''),''1'',''1'',''0'') = '''||issueflag||''' ))
                                   OR (T.SSYSID = ''TZHBJJ'' AND EXISTS (SELECT 1 FROM RWA.RWA_WS_BONDTRADE_MF T1 WHERE T.COLLATERALID = T1.MITIGATIONID AND T.DATADATE = T1.DATADATE AND DECODE(NVL(T1.LCISSUERTYPE,''02''),''01'',''1'',''0'') = '''||issueflag||''' ))) '
                  else ' ' end as issueflag
            --权重法.是否我行存单 码值:IssueOrg
            ,case when selfflag is not null
                  then ' AND ( (NVL(T.SSYSID,'' '') NOT IN (''LC'',''TZHBJJ'') AND EXISTS (SELECT 1 FROM RWA_DEV.NCM_ASSET_FINANCE T1 WHERE T.SCOLLATERALID = T1.GUARANTYID AND T.DATANO = T1.DATANO AND NVL(T1.ISOURS,''02'') = '''||selfflag||'''))
                                   OR (T.SSYSID = ''LC''  AND EXISTS (SELECT 1 FROM RWA_DEV.ZGS_ATINTRUST_PLAN T1 WHERE T.COLLATERALID = T1.C_PRD_CODE AND T.DATANO = T1.DATANO AND DECODE(NVL(T1.C_IS_BANK_RECEIPT,''2''),''1'',''01'',''02'') = '''||selfflag||''' ))
                                   OR (T.SSYSID = ''TZHBJJ'' AND EXISTS (SELECT 1 FROM RWA.RWA_WS_BONDTRADE_MF T1 WHERE T.COLLATERALID = T1.MITIGATIONID AND T.DATADATE = T1.DATADATE AND NVL(T1.LETTERTYPE,''02'') = '''||selfflag||''' ))) '
                  else ' ' end as selfflag
            --权重法.注册国家地区评级 码值:NSERatingGroup
            ,case when country_level_no is not null and country_level_no='01'-- AA-级以上
                  then ' AND T.RCERATING <= ''0104'' '
                  when country_level_no is not null and country_level_no='02'--AA-级以下，A-级及以上
                  then ' AND T.RCERating > ''0104'' AND T.RCERATING <= ''0107'' '
                  when country_level_no is not null and country_level_no='03'--A-级以下，BBB-级及以上
                  then ' AND T.RCERating > ''0107'' AND T.RCERATING <= ''0110'' '
                  when country_level_no is not null and country_level_no='04'--BBB-级以下，B-级及以上
                  then ' AND T.RCERating > ''0110'' AND T.RCERATING <= ''0116'' '
                  when country_level_no is not null and country_level_no='05'--B-级以下
                  then ' AND T.RCERATING > ''0116'' '
                  when country_level_no is not null and country_level_no='06'--未评级
                  then ' AND NVL(T.RCERATING,''0124'') IN (''0124'',''0207'') '
                  when country_level_no is not null and country_level_no='07'--A-级及以上
                  then ' AND T.RCERATING <= ''0107'' '
                  when country_level_no is not null and country_level_no='08'--BBB-级及以上
                  then ' AND T.RCERATING <= ''0110'' '
                  when country_level_no is not null and country_level_no='09'--B-级及以上，A-级以下
                  then ' AND T.RCERATING <=''0116'' AND T.RCERATING > ''0107'' '
                  else ' ' end as countrylevel
            --权重法.债券发行目的 码值:BondPublishPurpose
            ,case when issue_intent_type is not null
                  then ' AND T.SPECPURPBONDFLAG = DECODE('''||issue_intent_type||''',''0010'',''1'',''0'') '
                  else '' end as issue_intent_type
            ,collateral_std_detail
        from RWA_DEV.RWA_CD_COLLATERAL_STD  order by serial_no asc;

  v_cc_sdvs cc_sdvs%rowtype;

  BEGIN
    dbms_output.enable(20000);
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    --1.更新抵质押品表的合格认定状态为空
    --v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL SET QUALFLAGSTD = '''',COLLATERALTYPESTD='''',COLLATERALSDVSSTD='''',QUALFLAGFIRB = '''',COLLATERALTYPEIRB='''',FCTYPE='''' WHERE DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'') AND SSYSID <> ''HG''';
    --仅重置权重法部分，内评法从原系统获取，RWA系统仅兜底
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL SET QUALFLAGSTD = '''',COLLATERALTYPESTD='''',COLLATERALSDVSSTD='''' WHERE DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'') AND SSYSID <> ''HG''';
    v_tab_len := v_tab_len + 1;

    --2.获取待执行的更新sql
    --2.1 获取合格认定sql
    for v_cc_qlifd in cc_qlifd loop
        v_qualflagstd := '';
        v_qualflagfirb := '';
        v_clause :=  v_cc_qlifd.guaranty_kind4     --押品四类
                   ||v_cc_qlifd.issueflag          --权重法.是否我行发行 码值:YesOrNo
                   ||v_cc_qlifd.selfflag           --权重法.是否我行存单 码值:IssueOrg
                   ||v_cc_qlifd.countrylevel       --权重法.注册国家地区评级 码值:NSERatingGroup
                   ||v_cc_qlifd.issue_intent_type  --权重法.债券发行目的 码值:BondPublishPurpose
                   ||v_cc_qlifd.out_judege_type    --内评法.债券评级 码值:ERating
                   ||v_cc_qlifd.isratingupper      --内评法.发行人发行的同一级别债券外部评级是否BBB-或A-3/P-3及以上 码值:YesOrNo
                   ||v_cc_qlifd.tooltype           --内评法.投资金融工具类型 码值:YesOrNo
                   ||v_cc_qlifd.istransfer         --内评法.是否可转让 码值:YesOrNo
                   ||v_cc_qlifd.ispubliceveryvalue --内评法.是否每日公开报价 码值:YesOrNo
                   ||v_cc_qlifd.landproperty       --内评法.取得方式 码值:cms_code_library.CmsLandProperty01
                   ||v_cc_qlifd.rcvage;            --内评法.账龄
        if v_cc_qlifd.qualified_flag = '01' then     --T 权重法内评法都合格 代码:QualificationFlag
            v_qualflagstd := '1';
            v_qualflagfirb := '1';
        elsif v_cc_qlifd.qualified_flag = '02' then --T1 权重法不合格、内评法合格 代码:QualificationFlag
            v_qualflagstd := '0';
            v_qualflagfirb := '1';
        elsif v_cc_qlifd.qualified_flag = '03' then --F 权重法内评法都不合格 代码:QualificationFlag
            v_qualflagstd := '0';
            v_qualflagfirb := '0';
        elsif v_cc_qlifd.qualified_flag = '04' then --T2 权重法合格、内评法不合格 代码:QualificationFlag
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
    --添加剩余数据认定未不合格处理sql
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL T SET T.QUALFLAGSTD = ''0'',T.COLLATERALTYPESTD = '''',T.COLLATERALSDVSSTD = '''' WHERE T.QUALFLAGSTD IS NULL  AND T.SSYSID <> ''HG''AND T.DATADATE = TO_DATE('''|| p_data_dt_str ||''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL T SET T.QUALFLAGFIRB = ''0'',T.COLLATERALTYPEIRB = '''',T.FCTYPE = '''' WHERE T.QUALFLAGFIRB IS NULL  AND T.SSYSID <> ''HG''AND T.DATADATE = TO_DATE('''|| p_data_dt_str ||''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;

    --2.2 获取已合格抵质押品类型
    for v_cc_type in cc_type loop
        v_clause :=  v_cc_type.guaranty_kind4     --押品四类
                   ||v_cc_type.issueflag          --权重法.是否我行发行 码值:YesOrNo
                   ||v_cc_type.selfflag           --权重法.是否我行存单 码值:IssueOrg
                   ||v_cc_type.countrylevel       --权重法.注册国家地区评级 码值:NSERatingGroup
                   ||v_cc_type.issue_intent_type  --权重法.债券发行目的 码值:BondPublishPurpose
                   ||v_cc_type.out_judege_type    --内评法.债券评级 码值:ERating
                   ||v_cc_type.isratingupper      --内评法.发行人发行的同一级别债券外部评级是否BBB-或A-3/P-3及以上 码值:YesOrNo
                   ||v_cc_type.tooltype           --内评法.投资金融工具类型 码值:YesOrNo
                   ||v_cc_type.istransfer         --内评法.是否可转让 码值:YesOrNo
                   ||v_cc_type.ispubliceveryvalue --内评法.是否每日公开报价 码值:YesOrNo
                   ||v_cc_type.landproperty       --内评法.取得方式 码值:cms_code_library.CmsLandProperty01
                   ||v_cc_type.rcvage;            --内评法.账龄
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

    --2.3 获取已合格抵质押品细分
    for v_cc_sdvs in cc_sdvs loop
        v_clause :=  v_cc_sdvs.guaranty_kind4     --押品四类
                   ||v_cc_sdvs.issueflag          --权重法.是否我行发行 码值:YesOrNo
                   ||v_cc_sdvs.selfflag           --权重法.是否我行存单 码值:IssueOrg
                   ||v_cc_sdvs.countrylevel       --权重法.注册国家地区评级 码值:NSERatingGroup
                   ||v_cc_sdvs.issue_intent_type; --权重法.债券发行目的 码值:BondPublishPurpose
        v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL T
                                   SET T.COLLATERALSDVSSTD = ''' || v_cc_sdvs.collateral_std_detail || '''
                                 WHERE T.QUALFLAGSTD = ''1''
                                   AND T.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'') AND T.SSYSID <> ''HG'' '||v_clause;
        v_tab_len := v_tab_len + 1;
    end loop;

    --3.执行更新sql
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

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_COLLATERAL WHERE (QUALFLAGSTD IS NOT NULL OR QUALFLAGFIRB IS NOT NULL) AND DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_COLLATERAL表认定权重法/内评法下合格的抵质押品数据记录为: ' || v_count || ' 条');


    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功' || '-' || v_count;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
            --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
            Dbms_output.Put_line(v_err_sql);
            ROLLBACK;
            p_po_rtncode := sqlcode;
            p_po_rtnmsg  := '权重法/内评法合格抵质押品('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_CD_COLLATERAL;
/

