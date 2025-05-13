CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_HG_SFTDETAIL_BAK(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_HG_SFTDETAIL_BAK
    实现功能:核心系统-回购-证券融资交易相关信息(从数据源核心系统将债券信息全量导入RW回购接口表证券融资交易相关信息表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-04-18
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.BRD_REPO|债券回购
    源  表2 :RWA_DEV.BRD_SECURITY_POSI|债券头寸信息
    源  表3 :RWA_DEV.BRD_BOND|债券信息

    目标表  :RWA_DEV.RWA_HG_SFTDETAIL|核心系统回购类证券融资交易相关信息表
    变更记录(修改人|修改时间|修改内容):
    PXL 2019/04/15 去除老核心系统相关表，去除补录相关表
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_HG_SFTDETAIL_BAK';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;
  --v_count1 INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_HG_SFTDETAIL';

    --2.将满足条件的数据从源表插入到目标表中
    --2.1 核心系统-买入返售债券回购-买断式-资金
    INSERT INTO RWA_DEV.RWA_HG_SFTDETAIL(
                 DataDate                              --数据日期
                ,DataNo                          	 		 --数据流水号
                ,SFTDetailID                     	 		 --证券融资交易明细ID
                ,SecuID                          	 		 --证券ID
                ,SSysID                          	 		 --源系统ID
                ,ExposureID                      	 		 --风险暴露ID
                ,MasterNetAgreeID                	 		 --净额结算主协议ID
                ,BookType                        	 		 --账户类别
                ,TranRole                        	 		 --交易角色
                ,TradingAssetType                	 		 --交易资产类型
                ,ClaimsLevel                     	 		 --债权级别
                ,QualFlagSTD                     	 		 --权重法合格标识
                ,QualFlagFIRB                    	 		 --内评初级法合格标识
                ,CollateralSdvsSTD              	 		 --权重法抵质押品细分
                ,StartDate                 				 		 --起始日期
                ,DueDate                         	 		 --到期日期
                ,OriginalMaturity                	 		 --原始期限
                ,ResidualM                       	 		 --剩余期限
                ,AssetBalance                    	 		 --资产余额
                ,AssetCurrency                   	 		 --资产币种
                ,AppZeroHaircutsFlag             	 		 --是否适用零折扣系数
                ,InteHaircutsFlag                	 		 --自行估计折扣系数标识
                ,InternalHc                        		 --内部折扣系数
                ,SecuIssuerID                    	 		 --证券发行人ID
                ,BondIssueIntent                 	 		 --债券发行目的
                ,FCType                          	 		 --金融质押品类型
                ,ABSFlag                         	 		 --资产证券化标识
                ,RatingDurationType              	 		 --评级期限类型
                ,SecuIssueRating                 	 		 --证券发行等级
                ,SecuRevaFrequency               	 		 --证券重估频率
                ,RCERating														 --发行人境外注册地外部评级
    )
    SELECT    
    TO_DATE(p_data_dt_str,'YYYYMMDD') , --数据日期        
    p_data_dt_str , --数据流水号       
    T1.ACCT_NO || 'MRFSZJ'  , --证券融资交易明细ID        
    T1.ACCT_NO || 'MRFSZJ'  , --证券ID        
    'HG'  , --源系统ID       
    T1.ACCT_NO  , --风险暴露ID        
    ''  , --净额结算主协议ID       
    '01'  , --账户类别        默认 银行账户(01)
    '01'  , --交易角色        默认 风险暴露(01)
    '01'  , --交易资产类型        默认 资金(01)
    '01'  , --债权级别        默认 高级债权(01)
    '1' , --权重法合格标识       默认 合格(1)
    '1' , --内评初级法合格标识       默认 合格(1)
    '1' , --权重法抵质押品细分       默认 现金类资产(01)
    T1.START_DT , --起始日期        
    T1.MATU_DT  , --到期日期        
    CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.START_DT,'YYYY-MM-DD')) / 365<0
                          THEN 0
                          ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.START_DT,'YYYY-MM-DD')) / 365
                    END  , --原始期限        单位：年
    CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                  THEN 0
                  ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
            END  , --剩余期限        单位：年
    T1.CASH_NOMINAL , --资产余额        表内资产总余额=表内余额+应收费用+正常利息+表内欠息, 对于证券融资交易，即为证券市值或回购金额
    T1.CASH_CCY_CD  , --资产币种        
    '0' , --是否适用零折扣系数       默认 否(0)
    '0' , --自行估计折扣系数标识        默认 否(0)
    NULL  , --内部折扣系数        默认 空
    T3.ISSUER_CODE  , --证券发行人ID       默认 空
    '02'  , --债券发行目的        默认 其他(02)
    '01'  , --金融质押品类型       默认 现金及现金等价物(01)
    '0'   , --资产证券化标识       默认 否(0)
    ''  , --评级期限类型        默认 空
    ''  , --证券发行等级        默认 空
    1 , --证券重估频率        默认 1天
    ''  --发行人境外注册地外部评级        默认 空
    FROM  RWA_DEV.BRD_REPO      T1      
    LEFT JOIN RWA_DEV.BRD_SECURITY_POSI     T2  ON T1.ACCT_NO = T2.ACCT_NO    
    LEFT JOIN RWA_DEV.BRD_BOND      T3  ON T2.SECURITY_REFERENCE = T3.BOND_ID   
   WHERE T1.CASH_NOMINAL <> 0
     --AND T1.CLIENT_PROPRIETARY = 'T'  --买断式  源表为空，暂时更改
     AND T1.REPO_TYPE IN ( '4', 'RB')  --买入返售
     AND T1.PRINCIPAL_GLNO LIKE '111103%'  --买入返售债券回购-买断式-资金
     ;

    COMMIT;

    --2.2 买入返售债券回购-买断式-证券
    INSERT INTO RWA_DEV.RWA_HG_SFTDETAIL(
                 DataDate                              --数据日期
                ,DataNo                          	 		 --数据流水号
                ,SFTDetailID                     	 		 --证券融资交易明细ID
                ,SecuID                          	 		 --证券ID
                ,SSysID                          	 		 --源系统ID
                ,ExposureID                      	 		 --风险暴露ID
                ,MasterNetAgreeID                	 		 --净额结算主协议ID
                ,BookType                        	 		 --账户类别
                ,TranRole                        	 		 --交易角色
                ,TradingAssetType                	 		 --交易资产类型
                ,ClaimsLevel                     	 		 --债权级别
                ,QualFlagSTD                     	 		 --权重法合格标识
                ,QualFlagFIRB                    	 		 --内评初级法合格标识
                ,CollateralSdvsSTD              	 		 --权重法抵质押品细分
                ,StartDate                 				 		 --起始日期
                ,DueDate                         	 		 --到期日期
                ,OriginalMaturity                	 		 --原始期限
                ,ResidualM                       	 		 --剩余期限
                ,AssetBalance                    	 		 --资产余额
                ,AssetCurrency                   	 		 --资产币种
                ,AppZeroHaircutsFlag             	 		 --是否适用零折扣系数
                ,InteHaircutsFlag                	 		 --自行估计折扣系数标识
                ,InternalHc                        		 --内部折扣系数
                ,SecuIssuerID                    	 		 --证券发行人ID
                ,BondIssueIntent                 	 		 --债券发行目的
                ,FCType                          	 		 --金融质押品类型
                ,ABSFlag                         	 		 --资产证券化标识
                ,RatingDurationType              	 		 --评级期限类型
                ,SecuIssueRating                 	 		 --证券发行等级
                ,SecuRevaFrequency               	 		 --证券重估频率
                ,RCERating														 --发行人境外注册地外部评级
    )
    SELECT
    TO_DATE(p_data_dt_str,'YYYYMMDD') , --数据日期        
    p_data_dt_str , --数据流水号       
    T1.ACCT_NO || 'MRFSZQ'  , --证券融资交易明细ID        
    T1.ACCT_NO || 'MRFSZQ'  , --证券ID        
    'HG'  , --源系统ID       
    T1.ACCT_NO  , --风险暴露ID        
    ''  , --净额结算主协议ID       
    '01'  , --账户类别        默认 银行账户(01)
    '02'  , --交易角色        默认 02
    '02'  , --交易资产类型        默认 02
    '01'  , --债权级别        默认 高级债权(01)
    '1' , --权重法合格标识       默认 合格(1)
    '1' , --内评初级法合格标识       默认 合格(1)
    '1' , --权重法抵质押品细分       默认 现金类资产(01)
    T1.START_DT , --起始日期        
    T1.MATU_DT  , --到期日期        
    CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.START_DT,'YYYY-MM-DD')) / 365<0
                          THEN 0
                          ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.START_DT,'YYYY-MM-DD')) / 365
                    END  , --原始期限        单位：年
    CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                  THEN 0
                  ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
            END  , --剩余期限        单位：年
    T1.CASH_NOMINAL , --资产余额        表内资产总余额=表内余额+应收费用+正常利息+表内欠息, 对于证券融资交易，即为证券市值或回购金额
    T1.CASH_CCY_CD  , --资产币种        
    '0' , --是否适用零折扣系数       默认 否(0)
    '0' , --自行估计折扣系数标识        默认 否(0)
    NULL  , --内部折扣系数        默认 空
    T3.ISSUER_CODE  , --证券发行人ID       默认 空
    '02'  , --债券发行目的        默认 其他(02)
    '09'  , --金融质押品类型       默认 现金及现金等价物(01) 
    '0'   , --资产证券化标识       默认 其他(09)
    ''  , --评级期限类型        默认 空
    ''  , --证券发行等级        默认 空
    1 , --证券重估频率        默认 1天
    ''  --发行人境外注册地外部评级        默认 空
    FROM  RWA_DEV.BRD_REPO T1      
    LEFT JOIN RWA_DEV.BRD_SECURITY_POSI T2  ON T1.ACCT_NO = T2.ACCT_NO    
    LEFT JOIN RWA_DEV.BRD_BOND T3  ON T2.SECURITY_REFERENCE = T3.BOND_ID   
    WHERE T1.CASH_NOMINAL <> 0
         --AND T1.CLIENT_PROPRIETARY = 'T'  --买断式  源表为空，暂时更改
         AND T1.REPO_TYPE IN ( '4', 'RB')  --买入返售
         AND T1.PRINCIPAL_GLNO LIKE '111103%'  --买入返售债券回购-买断式-债券
    ;

    COMMIT;

    --2.3 卖出回购债券回购-买断式-资金
    INSERT INTO RWA_DEV.RWA_HG_SFTDETAIL(
                 DataDate                              --数据日期
                ,DataNo                          	 		 --数据流水号
                ,SFTDetailID                     	 		 --证券融资交易明细ID
                ,SecuID                          	 		 --证券ID
                ,SSysID                          	 		 --源系统ID
                ,ExposureID                      	 		 --风险暴露ID
                ,MasterNetAgreeID                	 		 --净额结算主协议ID
                ,BookType                        	 		 --账户类别
                ,TranRole                        	 		 --交易角色
                ,TradingAssetType                	 		 --交易资产类型
                ,ClaimsLevel                     	 		 --债权级别
                ,QualFlagSTD                     	 		 --权重法合格标识
                ,QualFlagFIRB                    	 		 --内评初级法合格标识
                ,CollateralSdvsSTD              	 		 --权重法抵质押品细分
                ,StartDate                 				 		 --起始日期
                ,DueDate                         	 		 --到期日期
                ,OriginalMaturity                	 		 --原始期限
                ,ResidualM                       	 		 --剩余期限
                ,AssetBalance                    	 		 --资产余额
                ,AssetCurrency                   	 		 --资产币种
                ,AppZeroHaircutsFlag             	 		 --是否适用零折扣系数
                ,InteHaircutsFlag                	 		 --自行估计折扣系数标识
                ,InternalHc                        		 --内部折扣系数
                ,SecuIssuerID                    	 		 --证券发行人ID
                ,BondIssueIntent                 	 		 --债券发行目的
                ,FCType                          	 		 --金融质押品类型
                ,ABSFlag                         	 		 --资产证券化标识
                ,RatingDurationType              	 		 --评级期限类型
                ,SecuIssueRating                 	 		 --证券发行等级
                ,SecuRevaFrequency               	 		 --证券重估频率
                ,RCERating														 --发行人境外注册地外部评级
    )
    SELECT
          TO_DATE(p_data_dt_str,'YYYYMMDD') , --数据日期        
          p_data_dt_str , --数据流水号       
          T1.ACCT_NO || 'MCHGZJ'  , --证券融资交易明细ID        
          T1.ACCT_NO || 'MCHGZJ'  , --证券ID        
          'HG'  , --源系统ID       
          T1.ACCT_NO  , --风险暴露ID        
          ''  , --净额结算主协议ID       
          '01'  , --账户类别        默认 银行账户(01)
          '01'  , --交易角色        默认 风险暴露(01)
          '01'  , --交易资产类型        默认 资金(01)
          '01'  , --债权级别        默认 高级债权(01)
          '1' , --权重法合格标识       默认 合格(1)
          '1' , --内评初级法合格标识       默认 合格(1)
          '1' , --权重法抵质押品细分       默认 现金类资产(01)
          T1.START_DT , --起始日期        
          T1.MATU_DT  , --到期日期        
          CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.START_DT,'YYYY-MM-DD')) / 365<0
                                THEN 0
                                ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.START_DT,'YYYY-MM-DD')) / 365
                          END  , --原始期限        单位：年
          CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                        THEN 0
                        ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                  END  , --剩余期限        单位：年
          T1.CASH_NOMINAL , --资产余额        表内资产总余额=表内余额+应收费用+正常利息+表内欠息, 对于证券融资交易，即为证券市值或回购金额
          T1.CASH_CCY_CD  , --资产币种        
          '0' , --是否适用零折扣系数       默认 否(0)
          '0' , --自行估计折扣系数标识        默认 否(0)
          NULL  , --内部折扣系数        默认 空
          T3.ISSUER_CODE  , --证券发行人ID       默认 空
          '02'  , --债券发行目的        默认 其他(02)
          '09'  , --金融质押品类型       默认 其他(09)
          '0'   , --资产证券化标识       默认 否(0)
          ''  , --评级期限类型        默认 空
          ''  , --证券发行等级        默认 空
          1 , --证券重估频率        默认 1天
          ''  --发行人境外注册地外部评级        默认 空
      FROM  BRD_REPO      T1      
      LEFT JOIN   BRD_SECURITY_POSI     T2  ON T1.ACCT_NO = T2.ACCT_NO    
      LEFT JOIN   BRD_BOND      T3  ON T2.SECURITY_REFERENCE = T3.BOND_ID   
      WHERE T1.CASH_NOMINAL <> 0
           --AND T1.CLIENT_PROPRIETARY = 'T'  --买断式  源表为空，暂时更改
           AND T1.REPO_TYPE IN ( '2', 'RS')  --正回购
           AND T1.PRINCIPAL_GLNO LIKE '211103%'  --卖出回购债券回购-买断式-资金
           ;

    COMMIT;

    --2.4 卖出回购债券回购-买断式-证券
    INSERT INTO RWA_DEV.RWA_HG_SFTDETAIL(
                 DataDate                              --数据日期
                ,DataNo                          	 		 --数据流水号
                ,SFTDetailID                     	 		 --证券融资交易明细ID
                ,SecuID                          	 		 --证券ID
                ,SSysID                          	 		 --源系统ID
                ,ExposureID                      	 		 --风险暴露ID
                ,MasterNetAgreeID                	 		 --净额结算主协议ID
                ,BookType                        	 		 --账户类别
                ,TranRole                        	 		 --交易角色
                ,TradingAssetType                	 		 --交易资产类型
                ,ClaimsLevel                     	 		 --债权级别
                ,QualFlagSTD                     	 		 --权重法合格标识
                ,QualFlagFIRB                    	 		 --内评初级法合格标识
                ,CollateralSdvsSTD              	 		 --权重法抵质押品细分
                ,StartDate                 				 		 --起始日期
                ,DueDate                         	 		 --到期日期
                ,OriginalMaturity                	 		 --原始期限
                ,ResidualM                       	 		 --剩余期限
                ,AssetBalance                    	 		 --资产余额
                ,AssetCurrency                   	 		 --资产币种
                ,AppZeroHaircutsFlag             	 		 --是否适用零折扣系数
                ,InteHaircutsFlag                	 		 --自行估计折扣系数标识
                ,InternalHc                        		 --内部折扣系数
                ,SecuIssuerID                    	 		 --证券发行人ID
                ,BondIssueIntent                 	 		 --债券发行目的
                ,FCType                          	 		 --金融质押品类型
                ,ABSFlag                         	 		 --资产证券化标识
                ,RatingDurationType              	 		 --评级期限类型
                ,SecuIssueRating                 	 		 --证券发行等级
                ,SecuRevaFrequency               	 		 --证券重估频率
                ,RCERating														 --发行人境外注册地外部评级
    )
    SELECT
      TO_DATE(p_data_dt_str,'YYYYMMDD') , --数据日期        
      p_data_dt_str , --数据流水号       
      T1.ACCT_NO || 'MCHGZQ'  , --证券融资交易明细ID        
      T1.ACCT_NO || 'MCHGZQ'  , --证券ID        
      'HG'  , --源系统ID       
      T1.ACCT_NO  , --风险暴露ID        
      ''  , --净额结算主协议ID       
      '01'  , --账户类别        默认 银行账户(01)
      '02'  , --交易角色        默认 02
      '02'  , --交易资产类型        默认 02
      '01'  , --债权级别        默认 高级债权(01)
      '1' , --权重法合格标识       默认 合格(1)
      '1' , --内评初级法合格标识       默认 合格(1)
      '1' , --权重法抵质押品细分       默认 现金类资产(01)
      T1.START_DT , --起始日期        
      T1.MATU_DT  , --到期日期        
      CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.START_DT,'YYYY-MM-DD')) / 365<0
                            THEN 0
                            ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.START_DT,'YYYY-MM-DD')) / 365
                      END  , --原始期限        单位：年
      CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                    THEN 0
                    ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
              END  , --剩余期限        单位：年
      T1.CASH_NOMINAL , --资产余额        表内资产总余额=表内余额+应收费用+正常利息+表内欠息, 对于证券融资交易，即为证券市值或回购金额
      T1.CASH_CCY_CD  , --资产币种        
      '0' , --是否适用零折扣系数       默认 否(0)
      '0' , --自行估计折扣系数标识        默认 否(0)
      NULL  , --内部折扣系数        默认 空
      T3.ISSUER_CODE  , --证券发行人ID       默认 空
      '02'  , --债券发行目的        默认 其他(02)
      '01'  , --金融质押品类型       默认 现金及现金等价物(01) 
      '0'   , --资产证券化标识       默认 否(0)
      ''  , --评级期限类型        默认 空
      ''  , --证券发行等级        默认 空
      1 , --证券重估频率        默认 1天
      ''  --发行人境外注册地外部评级        默认 空
    FROM RWA_DEV.BRD_REPO      T1      
    LEFT JOIN RWA_DEV.BRD_SECURITY_POSI     T2  ON T1.ACCT_NO = T2.ACCT_NO    
    LEFT JOIN RWA_DEV.BRD_BOND      T3  ON T2.SECURITY_REFERENCE = T3.BOND_ID   
    WHERE T1.CASH_NOMINAL <> 0
     --AND T1.CLIENT_PROPRIETARY = 'T'  --买断式  --买断式  源表为空，暂时更改
     AND T1.REPO_TYPE IN ( '2', 'RS')  --正回购
     AND T1.PRINCIPAL_GLNO LIKE '211103%'  --卖出回购债券回购-买断式-证券				
    ;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_HG_SFTDETAIL',cascade => true);


    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_HG_SFTDETAIL;
    --Dbms_output.Put_line('RWA_DEV.RWA_HG_SFTDETAIL表当前插入的核心系统-买断回购-证券-债券-逆回购数据记录为: ' || (v_count1 - v_count) || ' 条');



    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '证券融资交易('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_HG_SFTDETAIL_BAK;
/

