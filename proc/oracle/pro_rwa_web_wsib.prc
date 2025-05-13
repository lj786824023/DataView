CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_WEB_WSIB(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_WEB_WSIB
    实现功能:RWA系统-页面补录相关-补录铺底(从数据源RWA系统将业务相关信息全量导入RWA页面相关补录表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-06-20
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.RWA_EI_UNCONSFIINVEST|股权投资明细表
    源  表2 :RWA_DEV.RWA_EI_PROFITDIST|利润分配方案表
    源  表3 :RWA_DEV.RWA_EI_TAXASSET|净递延税信息表
    源  表4 :RWA_DEV.RWA_EI_FAILEDTTC|二级资本工具表
    目标表1 :RWA_DEV.RWA_EI_UNCONSFIINVEST|股权投资明细表
    目标表2 :RWA_DEV.RWA_EI_PROFITDIST|利润分配方案表
    目标表3 :RWA_DEV.RWA_EI_TAXASSET|净递延税信息表
    目标表4 :RWA_DEV.RWA_EI_FAILEDTTC|二级资本工具表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_WEB_WSIB';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count1 INTEGER;
  v_count2 INTEGER;
  v_count3 INTEGER;
  v_count4 INTEGER;

  v_cur_cnt1 INTEGER;
  v_cur_cnt2 INTEGER;
  v_cur_cnt3 INTEGER;
  v_cur_cnt4 INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.统计目标表中的当期记录
    --股权投资明细表
    SELECT COUNT(1) INTO v_cur_cnt1 FROM RWA_DEV.RWA_EI_UNCONSFIINVEST WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    --利润分配方案表
    SELECT COUNT(1) INTO v_cur_cnt2 FROM RWA_DEV.RWA_EI_PROFITDIST WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    --净递延税信息表
    SELECT COUNT(1) INTO v_cur_cnt3 FROM RWA_DEV.RWA_EI_TAXASSET WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    --二级资本工具表
    SELECT COUNT(1) INTO v_cur_cnt4 FROM RWA_DEV.RWA_EI_FAILEDTTC WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    --2.将满足条件的数据从源表插入到目标表中
    --2.1 RWA系统-股权投资明细表
    INSERT INTO RWA_DEV.RWA_EI_UNCONSFIINVEST(
                SERIALNO                                 --流水号
                ,DATADATE                             	 --数据日期
                ,DATANO             										 --数据流水号
                ,INVESTEENAME                            --被投资单位名称
                ,ORGANIZATIONCODE                     	 --投资对象组织机构代码
                ,EQUITYINVESTTYPE                     	 --股权投资对象类型
                ,ORGID                                	 --持股机构
                ,EQUITYINVESTAMOUNT                   	 --未并表金融金融机构股权投资金额(未扣除部分)
                ,CTOCINVESTAMOUNT   										 --核心一级资本投资金额
                ,OTOCINVESTAMOUNT                        --其他一级资本投资金额
                ,TTCINVESTAMOUNT                         --二级资本投资金额
                ,CTOCGAP                                 --核心一级资本缺口
                ,OTOCGAP                                 --其他一级资本缺口
                ,TTCGAP                                  --二级资本缺口
                ,PROVISIONS                              --减值准备
                ,EQUITYINVESTEEPROP                      --占被投资单位权益比例
                ,SUBJECT                                 --科目
                ,CURRENCY                                --币种
                ,EQUITYNATURE                            --股权性质
                ,EQUITYINVESTCAUSE                       --股权投资形成原因
                ,CONSOLIDATEFLAG                         --是否纳入并表范围
                ,NOTCONSOLIDATECAUSE                     --不纳入并表的原因
                ,RISKCLASSIFY                            --风险分类
                ,BUSINESSLINE                            --条线
                ,INPUTUSERID                             --登记人ID
                ,INPUTORGID                              --登记机构ID
                ,INPUTTIME                               --登记时间
                ,UPDATEUSERID                            --更新人ID
                ,UPDATEORGID                             --更新机构ID
                ,UPDATETIME                              --更新时间
                ,CUSTID1                                 --客户编号
    )
    SELECT
                p_data_dt_str || lpad(rownum, 8, '0')    AS SERIALNO                      --流水号
                ,TO_DATE(p_data_dt_str,'YYYYMMDD')     	 AS DATADATE                      --数据日期
                ,p_data_dt_str       										 AS DATANO                        --数据流水号
                ,INVESTEENAME                            AS INVESTEENAME                  --被投资单位名称
                ,ORGANIZATIONCODE                     	 AS ORGANIZATIONCODE              --投资对象组织机构代码
                ,EQUITYINVESTTYPE                     	 AS EQUITYINVESTTYPE              --股权投资对象类型
                ,ORGID                                	 AS ORGID                         --持股机构
                ,EQUITYINVESTAMOUNT                   	 AS EQUITYINVESTAMOUNT            --未并表金融金融机构股权投资金额(未扣除部分)
                ,CTOCINVESTAMOUNT   										 AS CTOCINVESTAMOUNT              --核心一级资本投资金额
                ,OTOCINVESTAMOUNT                        AS OTOCINVESTAMOUNT              --其他一级资本投资金额
                ,TTCINVESTAMOUNT                         AS TTCINVESTAMOUNT               --二级资本投资金额
                ,CTOCGAP                                 AS CTOCGAP                       --核心一级资本缺口
                ,OTOCGAP                                 AS OTOCGAP                       --其他一级资本缺口
                ,TTCGAP                                  AS TTCGAP                        --二级资本缺口
                ,PROVISIONS                              AS PROVISIONS                    --减值准备
                ,EQUITYINVESTEEPROP                      AS EQUITYINVESTEEPROP            --占被投资单位权益比例
                ,SUBJECT                                 AS SUBJECT                       --科目
                ,CURRENCY                                AS CURRENCY                      --币种
                ,EQUITYNATURE                            AS EQUITYNATURE                  --股权性质
                ,EQUITYINVESTCAUSE                       AS EQUITYINVESTCAUSE             --股权投资形成原因
                ,CONSOLIDATEFLAG                         AS CONSOLIDATEFLAG               --是否纳入并表范围
                ,NOTCONSOLIDATECAUSE                     AS NOTCONSOLIDATECAUSE           --不纳入并表的原因
                ,RISKCLASSIFY                            AS RISKCLASSIFY                  --风险分类
                ,BUSINESSLINE                            AS BUSINESSLINE                  --条线
                ,INPUTUSERID                             AS INPUTUSERID                   --登记人ID
                ,INPUTORGID                              AS INPUTORGID                    --登记机构ID
                ,INPUTTIME                               AS INPUTTIME                     --登记时间
                ,UPDATEUSERID                            AS UPDATEUSERID                  --更新人ID
                ,UPDATEORGID                             AS UPDATEORGID                   --更新机构ID
                ,UPDATETIME                              AS UPDATETIME                    --更新时间
                ,CUSTID1                                 AS CUSTID1                       --客户编号

    FROM				RWA_DEV.RWA_EI_UNCONSFIINVEST
		WHERE 			DATADATE = (SELECT MAX(DATADATE) FROM RWA_DEV.RWA_EI_UNCONSFIINVEST WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
		AND					v_cur_cnt1 = 0
		ORDER BY 		SERIALNO
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_UNCONSFIINVEST',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_EI_UNCONSFIINVEST WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD') AND SERIALNO LIKE p_data_dt_str || '________';
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_UNCONSFIINVEST表当前插入的RWA系统-股权投资明细铺底数据记录为: ' || v_count1 || ' 条');


    --2.2 RWA系统-利润分配方案表
    INSERT INTO RWA_DEV.RWA_EI_PROFITDIST(
                SERIALNO                               --流水号
                ,DATADATE                            	 --数据日期
                ,DATANO                                --数据流水号
                ,SBDUDPROFITS       									 --应分未分利润
                ,ILDDEBT                               --无形资产中土地使用权对应部分的递延税负债
                ,SHARESBALANCE                         --股份余额
                ,TYBPROFITDISTSHARES                   --前一年利润分配股数
                ,HOLDINGTIME                           --持股时间
                ,INPUTUSERID                           --登记人ID
                ,INPUTORGID                            --登记机构ID
                ,INPUTTIME                             --登记时间
                ,UPDATEUSERID                          --更新人ID
                ,UPDATEORGID                           --更新机构ID
                ,UPDATETIME                            --更新时间
    )
    SELECT
                p_data_dt_str || lpad(rownum, 8, '0')    AS SERIALNO                      --流水号
                ,TO_DATE(p_data_dt_str,'YYYYMMDD')     	 AS DATADATE                      --数据日期
                ,p_data_dt_str       										 AS DATANO                        --数据流水号
                ,SBDUDPROFITS                            AS SBDUDPROFITS       						--应分未分利润
                ,ILDDEBT            				             AS ILDDEBT                       --无形资产中土地使用权对应部分的递延税负债
                ,SHARESBALANCE      				             AS SHARESBALANCE                 --股份余额
                ,TYBPROFITDISTSHARES                     AS TYBPROFITDISTSHARES           --前一年利润分配股数
                ,HOLDINGTIME                             AS HOLDINGTIME                   --持股时间
                ,INPUTUSERID                             AS INPUTUSERID                   --登记人ID
                ,INPUTORGID                              AS INPUTORGID                    --登记机构ID
                ,INPUTTIME          										 AS INPUTTIME                     --登记时间
                ,UPDATEUSERID                            AS UPDATEUSERID                  --更新人ID
                ,UPDATEORGID                             AS UPDATEORGID                   --更新机构ID
                ,UPDATETIME                              AS UPDATETIME                    --更新时间

    FROM				RWA_DEV.RWA_EI_PROFITDIST
		WHERE 			DATADATE = (SELECT MAX(DATADATE) FROM RWA_DEV.RWA_EI_PROFITDIST WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
		AND					v_cur_cnt2 = 0
		ORDER BY 		SERIALNO
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_PROFITDIST',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count2 FROM RWA_DEV.RWA_EI_PROFITDIST WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD') AND SERIALNO LIKE p_data_dt_str || '________';
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_PROFITDIST表当前插入的RWA系统-利润分配方案铺底数据记录为: ' || v_count2 || ' 条');

    --2.3 RWA系统-净递延税信息表
    INSERT INTO RWA_DEV.RWA_EI_TAXASSET(
                SERIALNO                                 --流水号
                ,DATADATE                             	 --数据日期
                ,DATANO             										 --数据流水号
                ,TAXTYPE                             		 --净递延税类型
                ,ASSET                            	 		 --净递延税资产
                ,DEBT                             	 		 --净递延税负债
                ,VALIDATEFLAG                     	 		 --校验结果
                ,INPUTUSERID                      	 		 --登记人
                ,INPUTORGID     										 		 --登记机构
                ,INPUTTIME                           		 --登记时间
                ,UPDATEUSERID                        		 --更新人
                ,UPDATEORGID                         		 --更新机构
                ,UPDATETIME                          		 --更新时间
                ,DKZCJZZB_D                          		 --贷款资产减值损失(全口径+贴现资产利息调整)资产
                ,DKZCJZZB_C                          		 --贷款资产减值损失(全口径+贴现资产利息调整)负债
                ,CQGQTZ_D                            		 --长期股权投资 资产
                ,CQGQTZ_C                            		 --长期股权投资 负债
                ,CFTYKX_D                            		 --存放同业款项 资产
                ,CFTYKX_C                            		 --存放同业款项 负债
                ,YSLX_D                              		 --应收利息 资产
                ,YSLX_C                              		 --应收利息 负债
                ,QTYSK_D                             		 --其他应收款及应收款项投资 资产
                ,QTYSK_C                             		 --其他应收款及应收款项投资 负债
                ,DZZC_D                              		 --抵债资产 资产
                ,DZZC_C                              		 --抵债资产 负债
                ,JYXJRZCGYJZBD_D                     		 --交易性金融资产（公允价值变动） 资产
                ,JYXJRZCGYJZBD_C                     		 --交易性金融资产（公允价值变动） 负债
                ,JYXJRZC_D                           		 --交易性金融资产 资产
                ,JYXJRZC_C                           		 --交易性金融资产 负债
                ,ZQTZGYJZBD_D                        		 --可供出售金融资产-债券投资(公允价值变动) 资产
                ,ZQTZGYJZBD_C                        		 --可供出售金融资产-债券投资(公允价值变动) 负债
                ,ZQTZ_D         												 --可供出售金融资产-债券投资 资产
                ,ZQTZ_C                                  --可供出售金融资产-债券投资 负债
                ,QTGQGYJZBD_D                            --可供出售金融资产-(其他股权公允价值变动) 资产
                ,QTGQGYJZBD_C                            --可供出售金融资产-(其他股权公允价值变动) 负债
                ,QTGQ_D                                  --可供出售金融资产-其他股权 资产
                ,QTGQ_C                                  --可供出售金融资产-其他股权 负债
                ,CYZDQJRZC_D                             --持有至到期金融资产 资产
                ,CYZDQJRZC_C                             --持有至到期金融资产 负债
                ,YJFZ_D                                  --预计负债-应付内退职工薪酬 资产
                ,YJFZ_C                                  --预计负债-应付内退职工薪酬 负债
                ,YFGZJJJ_D                               --应付工资及奖金 资产
                ,YFGZJJJ_C                               --应付工资及奖金 负债
                ,YFZFGJJ_D                               --应付住房公积金 资产
                ,YFZFGJJ_C                               --应付住房公积金 负债
                ,YFNJ_D                                  --应付年金 资产
                ,YFNJ_C                                  --应付年金 负债
                ,YFGHJF_D                                --应付工会经费 资产
                ,YFGHJF_C                                --应付工会经费 负债
                ,YFJBSHBX_D                              --应付基本社会保险 资产
                ,YFJBSHBX_C                              --应付基本社会保险 负债
                ,YSLCSXFSRZSK_D                          --应收理财手续费收入暂收款资产
                ,YSLCSXFSRZSK_C                          --应收理财手续费收入暂收款负债
                ,ZSCWGWF_D                               --暂收财务顾问费资产
                ,ZSCWGWF_C                               --暂收财务顾问费负债
                ,QTYSKQT_D                               --其他应收款-其他资产
                ,QTYSKQT_C                               --其他应收款-其他负债
                ,MRFSJRZCLXTZ_D                          --买入返售金融资产-利息调整资产
                ,MRFSJRZCLXTZ_C                          --买入返售金融资产-利息调整负债
                ,TXZCLXTZ_D                              --贴现资产-利息调整资产
                ,TXZCLXTZ_C                              --贴现资产-利息调整负债
                ,MCHGJRZCLXTZ_D                          --卖出回购金融资产-利息调整资产
                ,MCHGJRZCLXTZ_C                          --卖出回购金融资产-利息调整负债
                ,WAQZFSZQRCB_D                           --未按权责发生制确认成本资产
                ,WAQZFSZQRCB_C                           --未按权责发生制确认成本负债
    )
    SELECT
                p_data_dt_str || lpad(rownum, 8, '0')    AS SERIALNO                      --流水号
                ,TO_DATE(p_data_dt_str,'YYYYMMDD')     	 AS DATADATE                      --数据日期
                ,p_data_dt_str       										 AS DATANO                        --数据流水号
                ,TAXTYPE                             		 AS TAXTYPE                   		--净递延税类型
                ,ASSET                            	 		 AS ASSET                     		--净递延税资产
                ,DEBT                             	 		 AS DEBT                      		--净递延税负债
                ,VALIDATEFLAG                     	 		 AS VALIDATEFLAG              		--校验结果
                ,INPUTUSERID                      	 		 AS INPUTUSERID               		--登记人
                ,INPUTORGID     										 		 AS INPUTORGID                		--登记机构
                ,INPUTTIME                           		 AS INPUTTIME                 		--登记时间
                ,UPDATEUSERID                        		 AS UPDATEUSERID              		--更新人
                ,UPDATEORGID                         		 AS UPDATEORGID               		--更新机构
                ,UPDATETIME                          		 AS UPDATETIME                		--更新时间
                ,DKZCJZZB_D                          		 AS DKZCJZZB_D                		--贷款资产减值损失(全口径+贴现资产利息调整)资产
                ,DKZCJZZB_C                          		 AS DKZCJZZB_C                		--贷款资产减值损失(全口径+贴现资产利息调整)负债
                ,CQGQTZ_D                            		 AS CQGQTZ_D                  		--长期股权投资 资产
                ,CQGQTZ_C                            		 AS CQGQTZ_C                  		--长期股权投资 负债
                ,CFTYKX_D                            		 AS CFTYKX_D                  		--存放同业款项 资产
                ,CFTYKX_C                            		 AS CFTYKX_C                  		--存放同业款项 负债
                ,YSLX_D                              		 AS YSLX_D                    		--应收利息 资产
                ,YSLX_C                              		 AS YSLX_C                    		--应收利息 负债
                ,QTYSK_D                             		 AS QTYSK_D                   		--其他应收款及应收款项投资 资产
                ,QTYSK_C                             		 AS QTYSK_C                   		--其他应收款及应收款项投资 负债
                ,DZZC_D                              		 AS DZZC_D                    		--抵债资产 资产
                ,DZZC_C                              		 AS DZZC_C                    		--抵债资产 负债
                ,JYXJRZCGYJZBD_D                     		 AS JYXJRZCGYJZBD_D           		--交易性金融资产（公允价值变动） 资产
                ,JYXJRZCGYJZBD_C                     		 AS JYXJRZCGYJZBD_C           		--交易性金融资产（公允价值变动） 负债
                ,JYXJRZC_D                           		 AS JYXJRZC_D                 		--交易性金融资产 资产
                ,JYXJRZC_C                           		 AS JYXJRZC_C                 		--交易性金融资产 负债
                ,ZQTZGYJZBD_D                        		 AS ZQTZGYJZBD_D              		--可供出售金融资产-债券投资(公允价值变动) 资产
                ,ZQTZGYJZBD_C                        		 AS ZQTZGYJZBD_C              		--可供出售金融资产-债券投资(公允价值变动) 负债
                ,ZQTZ_D         												 AS ZQTZ_D         								--可供出售金融资产-债券投资 资产
                ,ZQTZ_C                                  AS ZQTZ_C                        --可供出售金融资产-债券投资 负债
                ,QTGQGYJZBD_D                            AS QTGQGYJZBD_D                  --可供出售金融资产-(其他股权公允价值变动) 资产
                ,QTGQGYJZBD_C                            AS QTGQGYJZBD_C                  --可供出售金融资产-(其他股权公允价值变动) 负债
                ,QTGQ_D                                  AS QTGQ_D                        --可供出售金融资产-其他股权 资产
                ,QTGQ_C                                  AS QTGQ_C                        --可供出售金融资产-其他股权 负债
                ,CYZDQJRZC_D                             AS CYZDQJRZC_D                   --持有至到期金融资产 资产
                ,CYZDQJRZC_C                             AS CYZDQJRZC_C                   --持有至到期金融资产 负债
                ,YJFZ_D                                  AS YJFZ_D                        --预计负债-应付内退职工薪酬 资产
                ,YJFZ_C                                  AS YJFZ_C                        --预计负债-应付内退职工薪酬 负债
                ,YFGZJJJ_D                               AS YFGZJJJ_D                     --应付工资及奖金 资产
                ,YFGZJJJ_C                               AS YFGZJJJ_C                     --应付工资及奖金 负债
                ,YFZFGJJ_D                               AS YFZFGJJ_D                     --应付住房公积金 资产
                ,YFZFGJJ_C                               AS YFZFGJJ_C                     --应付住房公积金 负债
                ,YFNJ_D                                  AS YFNJ_D                        --应付年金 资产
                ,YFNJ_C                                  AS YFNJ_C                        --应付年金 负债
                ,YFGHJF_D                                AS YFGHJF_D                      --应付工会经费 资产
                ,YFGHJF_C                                AS YFGHJF_C                      --应付工会经费 负债
                ,YFJBSHBX_D                              AS YFJBSHBX_D                    --应付基本社会保险 资产
                ,YFJBSHBX_C                              AS YFJBSHBX_C                    --应付基本社会保险 负债
                ,YSLCSXFSRZSK_D                          AS YSLCSXFSRZSK_D                --应收理财手续费收入暂收款资产
                ,YSLCSXFSRZSK_C                          AS YSLCSXFSRZSK_C                --应收理财手续费收入暂收款负债
                ,ZSCWGWF_D                               AS ZSCWGWF_D                     --暂收财务顾问费资产
                ,ZSCWGWF_C                               AS ZSCWGWF_C                     --暂收财务顾问费负债
                ,QTYSKQT_D                               AS QTYSKQT_D                     --其他应收款-其他资产
                ,QTYSKQT_C                               AS QTYSKQT_C                     --其他应收款-其他负债
                ,MRFSJRZCLXTZ_D                          AS MRFSJRZCLXTZ_D                --买入返售金融资产-利息调整资产
                ,MRFSJRZCLXTZ_C                          AS MRFSJRZCLXTZ_C                --买入返售金融资产-利息调整负债
                ,TXZCLXTZ_D                              AS TXZCLXTZ_D                    --贴现资产-利息调整资产
                ,TXZCLXTZ_C                              AS TXZCLXTZ_C                    --贴现资产-利息调整负债
                ,MCHGJRZCLXTZ_D                          AS MCHGJRZCLXTZ_D                --卖出回购金融资产-利息调整资产
                ,MCHGJRZCLXTZ_C                          AS MCHGJRZCLXTZ_C                --卖出回购金融资产-利息调整负债
                ,WAQZFSZQRCB_D                           AS WAQZFSZQRCB_D                 --未按权责发生制确认成本资产
                ,WAQZFSZQRCB_C                           AS WAQZFSZQRCB_C                 --未按权责发生制确认成本负债

    FROM				RWA_DEV.RWA_EI_TAXASSET
		WHERE 			DATADATE = (SELECT MAX(DATADATE) FROM RWA_DEV.RWA_EI_TAXASSET WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
		AND					v_cur_cnt3 = 0
		ORDER BY 		SERIALNO
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_TAXASSET',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count3 FROM RWA_DEV.RWA_EI_TAXASSET WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD') AND SERIALNO LIKE p_data_dt_str || '________';
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_TAXASSET表当前插入的RWA系统-净递延税信息细铺底数据记录为: ' || v_count3 || ' 条');

    --2.4 RWA系统-二级资本工具表
    INSERT INTO RWA_DEV.RWA_EI_FAILEDTTC(
                SERIALNO                                 --流水号
                ,DATADATE                             	 --数据日期
                ,DATANO             										 --数据流水号
                ,BONDNAME                           		 --债券名称
                ,DENOMINATION                    	  		 --面额
                ,BOOKBALANCE                     	  		 --账面余额
                ,VALUEDATE                       	  		 --起息日
                ,REDEMPTIONDATE                  	  		 --赎回日
                ,HONOURDATE    										  		 --兑付日
                ,BONDCLASSIFY                       		 --债券分类
                ,RESIDUALM                          		 --剩余期限
                ,INPUTUSERID                        		 --登记人ID
                ,INPUTORGID                         		 --登记机构ID
                ,INPUTTIME                          		 --登记时间
                ,UPDATEUSERID                       		 --更新人ID
                ,UPDATEORGID                        		 --更新机构ID
                ,UPDATETIME                         		 --更新时间
                ,QUALFLAG                           		 --是否合格
    )
    SELECT
                p_data_dt_str || lpad(rownum, 8, '0')    AS SERIALNO                      --流水号
                ,TO_DATE(p_data_dt_str,'YYYYMMDD')     	 AS DATADATE                      --数据日期
                ,p_data_dt_str       										 AS DATANO                        --数据流水号
                ,BONDNAME                           		 AS BONDNAME                  		--债券名称
                ,DENOMINATION                    	  		 AS DENOMINATION              		--面额
                ,BOOKBALANCE                     	  		 AS BOOKBALANCE               		--账面余额
                ,VALUEDATE                       	  		 AS VALUEDATE                 		--起息日
                ,REDEMPTIONDATE                  	  		 AS REDEMPTIONDATE            		--赎回日
                ,HONOURDATE    										  		 AS HONOURDATE                		--兑付日
                ,BONDCLASSIFY                       		 AS BONDCLASSIFY              		--债券分类
                ,RESIDUALM                          		 AS RESIDUALM                 		--剩余期限
                ,INPUTUSERID                        		 AS INPUTUSERID               		--登记人ID
                ,INPUTORGID                         		 AS INPUTORGID                		--登记机构ID
                ,INPUTTIME                          		 AS INPUTTIME                 		--登记时间
                ,UPDATEUSERID                       		 AS UPDATEUSERID              		--更新人ID
                ,UPDATEORGID                        		 AS UPDATEORGID               		--更新机构ID
                ,UPDATETIME                         		 AS UPDATETIME                		--更新时间
                ,QUALFLAG                           		 AS QUALFLAG                  		--是否合格

    FROM				RWA_DEV.RWA_EI_FAILEDTTC
		WHERE 			DATADATE = (SELECT MAX(DATADATE) FROM RWA_DEV.RWA_EI_FAILEDTTC WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
		AND					v_cur_cnt4 = 0
		ORDER BY 		SERIALNO
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_FAILEDTTC',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count4 FROM RWA_DEV.RWA_EI_FAILEDTTC WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD') AND SERIALNO LIKE p_data_dt_str || '________';
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_FAILEDTTC表当前插入的RWA系统-二级资本工具信息铺底数据记录为: ' || v_count4 || ' 条');



    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || (v_count1 + v_count2 + v_count3 + v_count4);
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '页面补录数据铺底('|| v_pro_name ||')处理失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_WEB_WSIB;
/

