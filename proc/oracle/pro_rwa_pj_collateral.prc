CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_PJ_COLLATERAL(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_PJ_COLLATERAL
    实现功能:核心系统-票据贴现-抵质押品(从数据源核心系统将票据贴现相关信息全量导入RWA票据贴现接口表抵质押品表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-04-21
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.BRD_BILL|票据信息
    目标表  :RWA_DEV.RWA_PJ_COLLATERAL|票据贴现类抵质押品表
    变更记录(修改人|修改时间|修改内容):
    pxl 2019/04/16 去除相关补录、老核心表
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_PJ_COLLATERAL';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_PJ_COLLATERAL';

    /*有效借据下合同对应的抵质押品信息（转帖现_外转）*/
    INSERT INTO RWA_DEV.RWA_PJ_COLLATERAL(
                 DATADATE                                --数据日期
                ,DATANO                                 --数据流水号
                ,COLLATERALID                           --抵质押品ID
                ,SSYSID                                 --源系统ID
                ,SGUARCONTRACTID                        --源担保合同ID
                ,SCOLLATERALID                          --源抵质押品ID
                ,COLLATERALNAME                         --抵质押品名称
                ,ISSUERID                               --发行人ID
                ,PROVIDERID                             --提供人ID
                ,CREDITRISKDATATYPE                     --信用风险数据类型
                ,GUARANTEEWAY                            --担保方式
                ,SOURCECOLTYPE                          --源抵质押品大类
                ,SOURCECOLSUBTYPE                       --源抵质押品小类
                ,SPECPURPBONDFLAG                       --是否为收购国有银行不良贷款而发行的债券
                ,QUALFLAGSTD                            --权重法合格标识
                ,QUALFLAGFIRB                           --内评初级法合格标识
                ,COLLATERALTYPESTD                      --权重法抵质押品类型
                ,COLLATERALSDVSSTD                      --权重法抵质押品细分
                ,COLLATERALTYPEIRB                      --内评法抵质押品类型
                ,COLLATERALAMOUNT                        --抵押总额
                ,CURRENCY                               --币种
                ,STARTDATE                              --起始日期
                ,DUEDATE                                --到期日期
                ,ORIGINALMATURITY                       --原始期限
                ,RESIDUALM                              --剩余期限
                ,INTEHAIRCUTSFLAG                       --自行估计折扣系数标识
                ,INTERNALHC                             --内部折扣系数
                ,FCTYPE                                 --金融质押品类型
                ,ABSFLAG                                --资产证券化标识
                ,RATINGDURATIONTYPE                     --评级期限类型
                ,FCISSUERATING                          --金融质押品发行等级
                ,FCISSUERTYPE                           --金融质押品发行人类别
                ,FCISSUERSTATE                          --金融质押品发行人注册国家
                ,FCRESIDUALM                            --金融质押品剩余期限
                ,REVAFREQUENCY                          --重估频率
                ,GROUPID                                --分组编号
                ,RCERating                              --发行人境外注册地外部评级
    )
    SELECT
          TO_DATE(p_data_dt_str,'YYYYMMDD') , --数据日期        
          p_data_dt_str , --数据流水号       
          'PJ' || T1.CRDT_BIZ_ID  , --抵质押品ID        
          'PJ'  , --源系统ID       
          'PJ' || T1.CRDT_BIZ_ID  , --源担保合同ID       
          'PJ' || T1.CRDT_BIZ_ID  , --源抵质押品ID       
          CASE WHEN SUBSTR(T1.SBJT_CD, 1, 6)='130102' THEN '商业承兑汇票'
               ELSE '中国商业银行承兑汇票' 
          END, --抵质押品名称        
          T1.CUST_NO , --发行人ID       待数据验证
          T1.CUST_NO , --提供人ID       待数据验证
          '01'  , --信用风险数据类型        
          '060' , --担保方式        
          '001004'  , --源抵质押品大类       
          CASE WHEN SUBSTR(T1.SBJT_CD, 1, 6)='130102' THEN '001004004001'
               ELSE '001004002001'  
          END , --源抵质押品小类       
          '0' , --是否为收购国有银行不良贷款而发行的债券       
          ''  , --权重法合格标识       
          ''  , --内评初级法合格标识       
          ''  , --权重法抵质押品类型       
          ''  , --权重法抵质押品细分       
          ''  , --内评法抵质押品类型       
          T1.BILL_AMT , --抵押总额        
          NVL(T1.CCY_CD,'CNY') , --币种        
          T1.ISSUE_DT , --起始日期        
          T1.MATU_DT  , --到期日期        
          CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.ISSUE_DT,'YYYY-MM-DD')) / 365<0
                                THEN 0
                                ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.ISSUE_DT,'YYYY-MM-DD')) / 365
                          END  , --原始期限        
          CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                        THEN 0
                        ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                  END  , --剩余期限        
          '0' , --自行估计折扣系数标识        
          1 , --内部折扣系数        
          ''  , --金融质押品类型       
          '0' , --资产证券化标识       
          ''  , --评级期限类型        
          ''  , --金融质押品发行等级       
          '02'  , --金融质押品发行人类别        
          --'CHN' , --金融质押品发行人注册国家        
          '01',--中国
          CASE WHEN (TO_DATE(T1.DISC_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                        THEN 0
                        ELSE (TO_DATE(T1.DISC_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                  END  , --金融质押品剩余期限       
          1 , --重估频率        
          ''  , --分组编号        
          ''    --发行人境外注册地外部评级        无，目前票据业务都是人民币业务
    FROM	BRD_BILL			T1
    WHERE T1.ATL_PAY_AMT <> 0 --取的都是本金
            AND SUBSTR(T1.SBJT_CD, 1, 6) IN (
                '130101', --贴现资产-银行承兑汇票贴现
                '130103', --贴现资产-银行承兑汇票转贴现
                 '130102' --商业汇票贴现         转帖现已取到承兑行，不需要票据作为缓释
            )
            AND T1.DATANO=p_data_dt_str;
    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_PJ_COLLATERAL',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_PJ_COLLATERAL;

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '票据转贴现抵质押品('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_PJ_COLLATERAL;
/

