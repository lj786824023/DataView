CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_COLLATERAL_STD(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_CD_COLLATERAL_STD
    实现功能:RWA系统-合规数据集-权重法合格缓释物认定(抵质押品)
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
    */
  Authid Current_User
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_CD_COLLATERAL_STD';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;
  --定义varchar2(4000)数组
  type varchartab is table of varchar2(4000) index by binary_integer;
  v_sqlTab varchartab;
  --定义数组最大长度
  v_tab_len INTEGER := 1;
  --定义游标,权重法合格缓释物认定规则(老押品目录)
  cursor cc_qlifd is
  	select guaranty_kind2, --押品二类代码
           qualified_flag  --合格标识 码值:QualificationFlag
      from RWA_DEV.RWA_CD_COLLATERAL_QUALIFIED;  --抵质押品合格映射表

  v_cc_qlifd cc_qlifd%rowtype;
  --定义游标,权重法合格缓释物类型规则(老押品目录)
  cursor cc_type is
  	select guaranty_kind2, --押品二类代码
           std_code        --权重法抵质押品类型代码
      from RWA_DEV.RWA_CD_COLLATERAL_TYPE;  --抵质押品类型代码表

  v_cc_type cc_type%rowtype;
  --定义游标,权重法合格缓释物细分规则(老押品目录)
  cursor cc_sdvs is
  	select guaranty_kind2, --押品二类代码
    collateral_std_detail  --权重法缓释物细分
      from RWA_DEV.RWA_CD_COLLATERAL_STD; --合格抵质押品权重法映射表

  v_cc_sdvs cc_sdvs%rowtype;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    --1.更新抵质押品表的合格认定状态为空
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL SET QUALFLAGSTD = '''' WHERE SOURCECOLSUBTYPE IS NOT NULL AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;

    --2.获取待执行的更新sql
    --2.1 获取合格认定sql
    for v_cc_qlifd in cc_qlifd loop
    	if v_cc_qlifd.qualified_flag <> '01' then
    		v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL SET QUALFLAGSTD = ''0'', COLLATERALTYPESTD = '''', COLLATERALSDVSSTD = '''' WHERE SOURCECOLSUBTYPE = ''' || v_cc_qlifd.guaranty_kind2 || ''' AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    	else
    		v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL SET QUALFLAGSTD = ''1'', COLLATERALTYPESTD = '''', COLLATERALSDVSSTD = '''' WHERE SOURCECOLSUBTYPE = ''' || v_cc_qlifd.guaranty_kind2 || ''' AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    	end if;
    	v_tab_len := v_tab_len + 1;
    end loop;
    --添加剩余数据认定未不合格处理sql
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL SET QUALFLAGSTD = ''0'', COLLATERALTYPESTD = '''', COLLATERALSDVSSTD = '''' WHERE SOURCECOLSUBTYPE IS NOT NULL AND QUALFLAGSTD IS NULL AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;

    --2.2 获取已合格抵质押品类型
    for v_cc_type in cc_type loop
    	v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL SET COLLATERALTYPESTD = ''' || v_cc_type.std_code || ''' WHERE SOURCECOLSUBTYPE = ''' || v_cc_type.guaranty_kind2 || ''' AND QUALFLAGSTD = ''1'' AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    	v_tab_len := v_tab_len + 1;
    end loop;

    --2.3 获取已合格抵质押品细分
    for v_cc_sdvs in cc_sdvs loop
    	v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL SET COLLATERALSDVSSTD = ''' || v_cc_sdvs.collateral_std_detail || ''' WHERE SOURCECOLSUBTYPE = ''' || v_cc_sdvs.guaranty_kind2 || ''' AND QUALFLAGSTD = ''1'' AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    	v_tab_len := v_tab_len + 1;
    end loop;

    --3.执行更新sql
    for i in 1..v_sqlTab.count loop
    	if v_sqlTab(i) IS NOT NULL THEN
	    	EXECUTE IMMEDIATE v_sqlTab(i);

	    	COMMIT;
    	end if;
    end loop;

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_COLLATERAL WHERE QUALFLAGSTD = '1' AND SOURCECOLSUBTYPE IS NOT NULL AND DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_COLLATERAL表认定权重法下合格的抵质押品数据记录为: ' || v_count || ' 条');


    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '权重法合格抵质押品('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_CD_COLLATERAL_STD;
/

