CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_TZ_CLIENT_0808(p_data_dt_str IN  VARCHAR2, --数据日期
                                              p_po_rtncode  OUT VARCHAR2, --返回编号
                                              p_po_rtnmsg   OUT VARCHAR2  --返回描述
)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_TZ_CLIENT_0808
    实现功能:财务系统，将相关信息全量导入RWA接口表参与主体中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :QHJIANG
    编写时间:2016-04-14
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.ZGS_ATBOND|债券信息表
    源  表2 :RWA_DEV.ZGS_ATINTRUST_PLAN|资产管理计划表
    源  表3 :RWA_DEV.ZGS_INVESTASSETDETAIL|交易明细表
    源  表4 :RWA_DEV.ZGS_FINANCING_INFO|产品信息表
    目标表1 :RWA_DEV.RWA_TZ_CLIENT|RWA参与主体信息表
    辅助表  :无
    变更记录(修改人|修改时间|修改内容):
    */

  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_TZ_CLIENT_0808';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;


  BEGIN
    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --清除目标表中的原有记录
    --DELETE FROM RWA_DEV.RWA_TZ_CLIENT WHERE DATADATE=TO_DATE(p_data_dt_str,'yyyyMMdd');
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TZ_CLIENT';

    --人民币债券投资-国债发行人默认，强制插入国债发行人为中央政府
    INSERT INTO RWA_DEV.RWA_TZ_CLIENT(
         DataDate                   --数据日期
        ,DataNo                     --数据流水号
        ,ClientID                   --参与主体ID
        ,SourceClientID             --源参与主体ID
        ,SSysID                     --源系统ID
        ,ClientName                 --参与主体名称
        ,SOrgID                     --源机构ID
        ,SOrgName                   --源机构名称
        ,OrgSortNo                  --所属机构排序号
        ,OrgID                      --所属机构ID
        ,OrgName                    --所属机构名称
        ,IndustryID                 --所属行业代码
        ,IndustryName               --所属行业名称
        ,ClientType                 --参与主体大类
        ,ClientSubType              --参与主体小类
        ,RegistState                --注册国家或地区
        ,RCERating                  --境外注册地外部评级
        ,RCERAgency                 --境外注册地外部评级机构
        ,OrganizationCode           --组织机构代码
        ,ConsolidatedSCFlag         --是否并表子公司
        ,SLClientFlag               --专业贷款客户标识
        ,SLClientType               --专业贷款客户类型
        ,ExpoCategoryIRB            --内评法暴露类别
        ,ModelID                    --模型ID
        ,ModelIRating               --模型内部评级
        ,ModelPD                    --模型违约概率
        ,IRating                    --内部评级
        ,PD                         --违约概率
        ,DefaultFlag                --违约标识
        ,NewDefaultFlag             --新增违约标识
        ,DefaultDate                --违约时点
        ,ClientERating              --参与主体外部评级
        ,CCPFlag                    --中央交易对手标识
        ,QualCCPFlag                --是否合格中央交易对手
        ,ClearMemberFlag            --清算会员标识
        ,CompanySize                --企业规模
        ,SSMBFlag                   --标准小微企业标识
        ,SSMBFLAGSTD         				--权重法标准小微企业标识
        ,AnnualSale                 --公司客户年销售额
        ,CountryCode                --注册国家代码
        ,MSMBFlag										--工信部微小企业标识
    )
    SELECT
        				 TO_DATE(p_data_dt_str,'yyyyMMdd')     																	AS DataDate            		--数据日期
        				,p_data_dt_str                         																	AS DataNo              		--数据流水号
        				,'ZGZYZF'																																AS ClientID            		--参与主体ID            默认 中国中央政府
        				,'ZGZYZF'																																AS SourceClientID      		--源参与主体ID
        				,'TZ'                                 																	AS SSysID              		--源系统ID
        				,'中国中央政府'																													AS ClientName          		--参与主体名称          默认 中国中央政府
        				,'10000000'							              																	AS SOrgID              		--源机构ID
        				,'重庆银行'																															AS SOrgName            		--源机构名称
        				,'1'																																		AS OrgSortNo           		--所属机构排序号
        				,'10000000'								             																	AS OrgID               		--所属机构ID
        				,'重庆银行'																															AS OrgName             		--所属机构名称
        				,'999999'														  																	AS IndustryID          		--所属行业代码          默认 999999-未知
        				,'未知'											          																	AS IndustryName        		--所属行业名称          默认 999999-未知
        				,'01'																																		AS ClientType          		--参与主体大类          默认 01-主权
        				,'0101'																																	AS ClientSubType       		--参与主体小类          默认 0101-中国中央政府
        				,'01'	                                 																	AS RegistState         		--注册国家或地区        默认 01-境内
        				,'0104'					                       																	AS RCERating           		--境外注册地外部评级
        				,'01'                                  																	AS RCERAgency          		--境外注册地外部评级机构
        				,'ZGZYZFZZJGDM'						             																	AS OrganizationCode    		--组织机构代码
        				,'0'	                                																	AS ConsolidatedSCFlag  		--是否并表子公司
        				,'0'                                  																	AS SLClientFlag        		--专业贷款客户标识
        				,''	                                  																	AS SLClientType        		--专业贷款客户类型
        				,'020101'                              																	AS ExpoCategoryIRB     		--内评法暴露类别        默认 020101-中央政府
        				,''				                            																	AS ModelID             		--模型ID
        				,''				                            																	AS ModelIRating        		--模型内部评级
        				,NULL                                 																	AS ModelPD             		--模型违约概率
        				,''								                    																	AS IRating             		--内部评级
        				,NULL	                                																	AS PD                  		--违约概率
        				,'0'																																		AS DefaultFlag         		--违约标识
        				,'0'											            																	AS NewDefaultFlag      		--新增违约标识
        				,''																     																	AS DefaultDate         		--违约时点
        				,'0104'														     																	AS ClientERating       		--参与主体外部评级
        				,'0'                                  																	AS CCPFlag             		--中央交易对手标识
        				,'0'                                   																	AS QualCCPFlag         		--是否合格中央交易对手
        				,'0'                                   																	AS ClearMemberFlag     		--清算会员标识
        				,'00'									                																	AS CompanySize         		--企业规模
        				,'0'											            																	AS SSMBFlag            		--标准小微企业标识
        				,'0'											            																	AS SSMBFlagSTD         		--权重法标准小微企业标识
        				,NULL							                     																	AS AnnualSale          		--公司客户年销售额
        				,'CHN'																																	AS CountryCode            --注册国家代码
        				,''																																			AS MSMBFlag								--工信部微小企业标识

    FROM				DUAL
	  ;

		COMMIT;

		--应收款投资-实际融资人默认，强制插入实际融资人为一般公司
		INSERT INTO RWA_DEV.RWA_TZ_CLIENT(
         DataDate                   --数据日期
        ,DataNo                     --数据流水号
        ,ClientID                   --参与主体ID
        ,SourceClientID             --源参与主体ID
        ,SSysID                     --源系统ID
        ,ClientName                 --参与主体名称
        ,SOrgID                     --源机构ID
        ,SOrgName                   --源机构名称
        ,OrgSortNo                  --所属机构排序号
        ,OrgID                      --所属机构ID
        ,OrgName                    --所属机构名称
        ,IndustryID                 --所属行业代码
        ,IndustryName               --所属行业名称
        ,ClientType                 --参与主体大类
        ,ClientSubType              --参与主体小类
        ,RegistState                --注册国家或地区
        ,RCERating                  --境外注册地外部评级
        ,RCERAgency                 --境外注册地外部评级机构
        ,OrganizationCode           --组织机构代码
        ,ConsolidatedSCFlag         --是否并表子公司
        ,SLClientFlag               --专业贷款客户标识
        ,SLClientType               --专业贷款客户类型
        ,ExpoCategoryIRB            --内评法暴露类别
        ,ModelID                    --模型ID
        ,ModelIRating               --模型内部评级
        ,ModelPD                    --模型违约概率
        ,IRating                    --内部评级
        ,PD                         --违约概率
        ,DefaultFlag                --违约标识
        ,NewDefaultFlag             --新增违约标识
        ,DefaultDate                --违约时点
        ,ClientERating              --参与主体外部评级
        ,CCPFlag                    --中央交易对手标识
        ,QualCCPFlag                --是否合格中央交易对手
        ,ClearMemberFlag            --清算会员标识
        ,CompanySize                --企业规模
        ,SSMBFlag                   --标准小微企业标识
        ,SSMBFLAGSTD         				--权重法标准小微企业标识
        ,AnnualSale                 --公司客户年销售额
        ,CountryCode                --注册国家代码
        ,MSMBFlag										--工信部微小企业标识
    )
    SELECT
        				 TO_DATE(p_data_dt_str,'yyyyMMdd')     																	AS DataDate            		--数据日期
        				,p_data_dt_str                         																	AS DataNo              		--数据流水号
        				,'XN-YBGS'																															AS ClientID            		--参与主体ID            默认 中国中央政府
        				,'XN-YBGS'																															AS SourceClientID      		--源参与主体ID
        				,'TZ'                                 																	AS SSysID              		--源系统ID
        				,'虚拟一般公司'																													AS ClientName          		--参与主体名称          默认 中国中央政府
        				,'10000000'							              																	AS SOrgID              		--源机构ID
        				,'重庆银行'																															AS SOrgName            		--源机构名称
        				,'1'																																		AS OrgSortNo           		--所属机构排序号
        				,'10000000'								             																	AS OrgID               		--所属机构ID
        				,'重庆银行'																															AS OrgName             		--所属机构名称
        				,'999999'														  																	AS IndustryID          		--所属行业代码          默认 999999-未知
        				,'未知'											          																	AS IndustryName        		--所属行业名称          默认 999999-未知
        				,'03'																																		AS ClientType          		--参与主体大类          默认 03-公司
        				,'0301'																																	AS ClientSubType       		--参与主体小类          默认 0301-一般公司
        				,'01'	                                 																	AS RegistState         		--注册国家或地区        默认 01-境内
        				,'0104'					                       																	AS RCERating           		--境外注册地外部评级
        				,'01'                                  																	AS RCERAgency          		--境外注册地外部评级机构
        				,'XNYBGSZZJGDM'						             																	AS OrganizationCode    		--组织机构代码
        				,'0'	                                																	AS ConsolidatedSCFlag  		--是否并表子公司
        				,'0'                                  																	AS SLClientFlag        		--专业贷款客户标识
        				,''	                                  																	AS SLClientType        		--专业贷款客户类型
        				,'020301'                              																	AS ExpoCategoryIRB     		--内评法暴露类别        默认 020301-一般公司
        				,''				                            																	AS ModelID             		--模型ID
        				,''				                            																	AS ModelIRating        		--模型内部评级
        				,NULL                                 																	AS ModelPD             		--模型违约概率
        				,''								                    																	AS IRating             		--内部评级
        				,NULL	                                																	AS PD                  		--违约概率
        				,'0'																																		AS DefaultFlag         		--违约标识
        				,'0'											            																	AS NewDefaultFlag      		--新增违约标识
        				,''																     																	AS DefaultDate         		--违约时点
        				,'0104'														     																	AS ClientERating       		--参与主体外部评级
        				,'0'                                  																	AS CCPFlag             		--中央交易对手标识
        				,'0'                                   																	AS QualCCPFlag         		--是否合格中央交易对手
        				,'0'                                   																	AS ClearMemberFlag     		--清算会员标识
        				,'00'									                																	AS CompanySize         		--企业规模
        				,'0'											            																	AS SSMBFlag            		--标准小微企业标识
        				,'0'											            																	AS SSMBFlagSTD         		--权重法标准小微企业标识
        				,NULL							                     																	AS AnnualSale          		--公司客户年销售额
        				,'CHN'																																	AS CountryCode            --注册国家代码
        				,''																																			AS MSMBFlag								--工信部微小企业标识

    FROM				DUAL
	  ;

		COMMIT;

		--票据(转)贴现-交易对手默认，强制插入交易对手为中国商业银行
		INSERT INTO RWA_DEV.RWA_TZ_CLIENT(
         DataDate                   --数据日期
        ,DataNo                     --数据流水号
        ,ClientID                   --参与主体ID
        ,SourceClientID             --源参与主体ID
        ,SSysID                     --源系统ID
        ,ClientName                 --参与主体名称
        ,SOrgID                     --源机构ID
        ,SOrgName                   --源机构名称
        ,OrgSortNo                  --所属机构排序号
        ,OrgID                      --所属机构ID
        ,OrgName                    --所属机构名称
        ,IndustryID                 --所属行业代码
        ,IndustryName               --所属行业名称
        ,ClientType                 --参与主体大类
        ,ClientSubType              --参与主体小类
        ,RegistState                --注册国家或地区
        ,RCERating                  --境外注册地外部评级
        ,RCERAgency                 --境外注册地外部评级机构
        ,OrganizationCode           --组织机构代码
        ,ConsolidatedSCFlag         --是否并表子公司
        ,SLClientFlag               --专业贷款客户标识
        ,SLClientType               --专业贷款客户类型
        ,ExpoCategoryIRB            --内评法暴露类别
        ,ModelID                    --模型ID
        ,ModelIRating               --模型内部评级
        ,ModelPD                    --模型违约概率
        ,IRating                    --内部评级
        ,PD                         --违约概率
        ,DefaultFlag                --违约标识
        ,NewDefaultFlag             --新增违约标识
        ,DefaultDate                --违约时点
        ,ClientERating              --参与主体外部评级
        ,CCPFlag                    --中央交易对手标识
        ,QualCCPFlag                --是否合格中央交易对手
        ,ClearMemberFlag            --清算会员标识
        ,CompanySize                --企业规模
        ,SSMBFlag                   --标准小微企业标识
        ,SSMBFLAGSTD         				--权重法标准小微企业标识
        ,AnnualSale                 --公司客户年销售额
        ,CountryCode                --注册国家代码
        ,MSMBFlag										--工信部微小企业标识
    )
    SELECT
        				 TO_DATE(p_data_dt_str,'yyyyMMdd')     																	AS DataDate            		--数据日期
        				,p_data_dt_str                         																	AS DataNo              		--数据流水号
        				,'XN-ZGSYYH'																														AS ClientID            		--参与主体ID            默认 中国中央政府
        				,'XN-ZGSYYH'																														AS SourceClientID      		--源参与主体ID
        				,'TZ'                                 																	AS SSysID              		--源系统ID
        				,'虚拟中国商业银行'																											AS ClientName          		--参与主体名称          默认 中国中央政府
        				,'10000000'							              																	AS SOrgID              		--源机构ID
        				,'重庆银行'																															AS SOrgName            		--源机构名称
        				,'1'																																		AS OrgSortNo           		--所属机构排序号
        				,'10000000'								             																	AS OrgID               		--所属机构ID
        				,'重庆银行'																															AS OrgName             		--所属机构名称
        				,'J6620'														  																	AS IndustryID          		--所属行业代码          默认 J6620-货币银行服务
        				,'货币银行服务'							          																	AS IndustryName        		--所属行业名称          默认 J6620-货币银行服务
        				,'02'																																		AS ClientType          		--参与主体大类          默认 02-金融机构
        				,'0202'																																	AS ClientSubType       		--参与主体小类          默认 0202-中国商业银行
        				,'01'	                                 																	AS RegistState         		--注册国家或地区        默认 01-境内
        				,'0104'					                       																	AS RCERating           		--境外注册地外部评级
        				,'01'                                  																	AS RCERAgency          		--境外注册地外部评级机构
        				,'XNZGSYYHZZJGDM'					             																	AS OrganizationCode    		--组织机构代码
        				,'0'	                                																	AS ConsolidatedSCFlag  		--是否并表子公司
        				,'0'                                  																	AS SLClientFlag        		--专业贷款客户标识
        				,''	                                  																	AS SLClientType        		--专业贷款客户类型
        				,'020201'                              																	AS ExpoCategoryIRB     		--内评法暴露类别        默认 020201-银行类金融机构
        				,''				                            																	AS ModelID             		--模型ID
        				,''				                            																	AS ModelIRating        		--模型内部评级
        				,NULL                                 																	AS ModelPD             		--模型违约概率
        				,''								                    																	AS IRating             		--内部评级
        				,NULL	                                																	AS PD                  		--违约概率
        				,'0'																																		AS DefaultFlag         		--违约标识
        				,'0'											            																	AS NewDefaultFlag      		--新增违约标识
        				,''																     																	AS DefaultDate         		--违约时点
        				,'0104'														     																	AS ClientERating       		--参与主体外部评级
        				,'0'                                  																	AS CCPFlag             		--中央交易对手标识
        				,'0'                                   																	AS QualCCPFlag         		--是否合格中央交易对手
        				,'0'                                   																	AS ClearMemberFlag     		--清算会员标识
        				,'00'									                																	AS CompanySize         		--企业规模
        				,'0'											            																	AS SSMBFlag            		--标准小微企业标识
        				,'0'											            																	AS SSMBFlagSTD         		--权重法标准小微企业标识
        				,NULL							                     																	AS AnnualSale          		--公司客户年销售额
        				,'CHN'																																	AS CountryCode            --注册国家代码
        				,''																																			AS MSMBFlag								--工信部微小企业标识

    FROM				DUAL
	  ;

		COMMIT;

		--信贷-交易对手默认，强制插入交易对手为个人
		INSERT INTO RWA_DEV.RWA_TZ_CLIENT(
         DataDate                   --数据日期
        ,DataNo                     --数据流水号
        ,ClientID                   --参与主体ID
        ,SourceClientID             --源参与主体ID
        ,SSysID                     --源系统ID
        ,ClientName                 --参与主体名称
        ,SOrgID                     --源机构ID
        ,SOrgName                   --源机构名称
        ,OrgSortNo                  --所属机构排序号
        ,OrgID                      --所属机构ID
        ,OrgName                    --所属机构名称
        ,IndustryID                 --所属行业代码
        ,IndustryName               --所属行业名称
        ,ClientType                 --参与主体大类
        ,ClientSubType              --参与主体小类
        ,RegistState                --注册国家或地区
        ,RCERating                  --境外注册地外部评级
        ,RCERAgency                 --境外注册地外部评级机构
        ,OrganizationCode           --组织机构代码
        ,ConsolidatedSCFlag         --是否并表子公司
        ,SLClientFlag               --专业贷款客户标识
        ,SLClientType               --专业贷款客户类型
        ,ExpoCategoryIRB            --内评法暴露类别
        ,ModelID                    --模型ID
        ,ModelIRating               --模型内部评级
        ,ModelPD                    --模型违约概率
        ,IRating                    --内部评级
        ,PD                         --违约概率
        ,DefaultFlag                --违约标识
        ,NewDefaultFlag             --新增违约标识
        ,DefaultDate                --违约时点
        ,ClientERating              --参与主体外部评级
        ,CCPFlag                    --中央交易对手标识
        ,QualCCPFlag                --是否合格中央交易对手
        ,ClearMemberFlag            --清算会员标识
        ,CompanySize                --企业规模
        ,SSMBFlag                   --标准小微企业标识
        ,SSMBFLAGSTD         				--权重法标准小微企业标识
        ,AnnualSale                 --公司客户年销售额
        ,CountryCode                --注册国家代码
        ,MSMBFlag										--工信部微小企业标识
    )
    SELECT
        				 TO_DATE(p_data_dt_str,'yyyyMMdd')     																	AS DataDate            		--数据日期
        				,p_data_dt_str                         																	AS DataNo              		--数据流水号
        				,'XN-GRKH'																															AS ClientID            		--参与主体ID            默认 个人客户
        				,'XN-GRKH'																															AS SourceClientID      		--源参与主体ID
        				,'TZ'                                 																	AS SSysID              		--源系统ID
        				,'虚拟个人客户'																													AS ClientName          		--参与主体名称          默认 个人客户
        				,'10000000'							              																	AS SOrgID              		--源机构ID
        				,'重庆银行'																															AS SOrgName            		--源机构名称
        				,'1'																																		AS OrgSortNo           		--所属机构排序号
        				,'10000000'								             																	AS OrgID               		--所属机构ID
        				,'重庆银行'																															AS OrgName             		--所属机构名称
        				,''																	  																	AS IndustryID          		--所属行业代码          默认 空
        				,''													          																	AS IndustryName        		--所属行业名称          默认 空
        				,'04'																																		AS ClientType          		--参与主体大类          默认 04-个人
        				,'0401'																																	AS ClientSubType       		--参与主体小类          默认 0401-个人（自然人）
        				,'01'	                                 																	AS RegistState         		--注册国家或地区        默认 01-境内
        				,''							                       																	AS RCERating           		--境外注册地外部评级    默认 空
        				,''	                                  																	AS RCERAgency          		--境外注册地外部评级机构 默认 空
        				,''												             																	AS OrganizationCode    		--组织机构代码          默认 空
        				,'0'	                                																	AS ConsolidatedSCFlag  		--是否并表子公司        默认 空
        				,'0'                                  																	AS SLClientFlag        		--专业贷款客户标识      默认 0-否
        				,''	                                  																	AS SLClientType        		--专业贷款客户类型      默认 0-否
        				,'020403'                              																	AS ExpoCategoryIRB     		--内评法暴露类别        默认 020403-其他零售
        				,''				                            																	AS ModelID             		--模型ID
        				,''				                            																	AS ModelIRating        		--模型内部评级
        				,NULL                                 																	AS ModelPD             		--模型违约概率
        				,''								                    																	AS IRating             		--内部评级
        				,NULL	                                																	AS PD                  		--违约概率
        				,'0'																																		AS DefaultFlag         		--违约标识
        				,'0'											            																	AS NewDefaultFlag      		--新增违约标识
        				,''																     																	AS DefaultDate         		--违约时点
        				,''																     																	AS ClientERating       		--参与主体外部评级
        				,'0'                                  																	AS CCPFlag             		--中央交易对手标识
        				,'0'                                   																	AS QualCCPFlag         		--是否合格中央交易对手
        				,'0'                                   																	AS ClearMemberFlag     		--清算会员标识
        				,'00'									                																	AS CompanySize         		--企业规模
        				,'0'											            																	AS SSMBFlag            		--标准小微企业标识
        				,'0'											            																	AS SSMBFlagSTD         		--权重法标准小微企业标识
        				,NULL							                     																	AS AnnualSale          		--公司客户年销售额
        				,'CHN'																																	AS CountryCode            --注册国家代码
        				,''																																			AS MSMBFlag								--工信部微小企业标识

    FROM				DUAL
	  ;

		COMMIT;

    --外币债券投资-主权类发行人默认
    INSERT INTO RWA_DEV.RWA_TZ_CLIENT(
         DataDate                   --数据日期
        ,DataNo                     --数据流水号
        ,ClientID                   --参与主体ID
        ,SourceClientID             --源参与主体ID
        ,SSysID                     --源系统ID
        ,ClientName                 --参与主体名称
        ,SOrgID                     --源机构ID
        ,SOrgName                   --源机构名称
        ,OrgSortNo                  --所属机构排序号
        ,OrgID                      --所属机构ID
        ,OrgName                    --所属机构名称
        ,IndustryID                 --所属行业代码
        ,IndustryName               --所属行业名称
        ,ClientType                 --参与主体大类
        ,ClientSubType              --参与主体小类
        ,RegistState                --注册国家或地区
        ,RCERating                  --境外注册地外部评级
        ,RCERAgency                 --境外注册地外部评级机构
        ,OrganizationCode           --组织机构代码
        ,ConsolidatedSCFlag         --是否并表子公司
        ,SLClientFlag               --专业贷款客户标识
        ,SLClientType               --专业贷款客户类型
        ,ExpoCategoryIRB            --内评法暴露类别
        ,ModelID                    --模型ID
        ,ModelIRating               --模型内部评级
        ,ModelPD                    --模型违约概率
        ,IRating                    --内部评级
        ,PD                         --违约概率
        ,DefaultFlag                --违约标识
        ,NewDefaultFlag             --新增违约标识
        ,DefaultDate                --违约时点
        ,ClientERating              --参与主体外部评级
        ,CCPFlag                    --中央交易对手标识
        ,QualCCPFlag                --是否合格中央交易对手
        ,ClearMemberFlag            --清算会员标识
        ,CompanySize                --企业规模
        ,SSMBFlag                   --标准小微企业标识
        ,SSMBFLAGSTD         				--权重法标准小微企业标识
        ,AnnualSale                 --公司客户年销售额
        ,CountryCode                --注册国家代码
        ,MSMBFlag										--工信部微小企业标识
    )
  	WITH TEMP_BND_BOOK AS (
    										SELECT BOND_ID,
												       INITIAL_COST,
												       INT_ADJUST,
												       MKT_VALUE_CHANGE,
												       RECEIVABLE_INT,
												       ACCOUNTABLE_INT
												  FROM (SELECT BOND_ID,
												               INITIAL_COST,
												               INT_ADJUST,
												               MKT_VALUE_CHANGE,
												               RECEIVABLE_INT,
												               ACCOUNTABLE_INT,
												               ROW_NUMBER() OVER(PARTITION BY BOND_ID ORDER BY SORT_SEQ DESC) RM
												          FROM FNS_BND_BOOK_B
												         WHERE AS_OF_DATE <= p_data_dt_str
												           AND DATANO = p_data_dt_str)
												 WHERE RM = 1
												   AND NVL(INITIAL_COST, 0) + NVL(INT_ADJUST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0
		)
		, TMP_BND_ORG AS (
									SELECT T4.BONDPUBLISHCOUNTRY		AS COUNTRYCODE
												,T4.MARKETSCATEGORY				AS MARKETSCATEGORY
												,MIN(T1.DEPARTMENT) 			AS ORGID
										FROM RWA_DEV.FNS_BND_INFO_B T1
							INNER JOIN TEMP_BND_BOOK T2
											ON T1.BOND_ID = T2.BOND_ID
							INNER JOIN RWA_DEV.NCM_BUSINESS_DUEBILL T3														--信贷借据表
											ON 'CW_IMPORTDATA' || T1.BOND_ID = T3.THIRDPARTYACCOUNTS
										 AND T3.BUSINESSTYPE = '1040202011'															--外币债券投资
										 AND T3.DATANO = p_data_dt_str
							INNER JOIN RWA_DEV.NCM_BOND_INFO T4																		--信贷债券信息表
	  									ON T3.RELATIVESERIALNO2 = T4.OBJECTNO
										 AND T4.OBJECTTYPE = 'BusinessContract'
										 AND T4.BONDFLAG04 = '1'																				--债券发行人是主权类
										 AND T4.DATANO = p_data_dt_str
	  							 WHERE (T1.ASSET_CLASS = '20' OR
								 					(T1.ASSET_CLASS = '40' AND T1.BOND_TYPE1 NOT IN ('091','099')) OR
								 					(T1.ASSET_CLASS = '40' AND T1.BOND_TYPE1 IN ('091','099') AND T1.CLOSED = '1')
												 )
	  																																--通过资产分类来确定债券还是应收款投资。
	  																																--40 可供出售类资产
														--进入合规数据集的债券投资范围，需要排除“债券分类”=‘资产支持证券’的记录。因为这一类单独通过补录模板获取数据。不要算两遍
										 AND T1.BOND_TYPE1 <> '060'
										 AND T1.BOND_ID NOT IN
													(SELECT ZQNM FROM RWA.RWA_WSIB_ABS_ISSUE_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
								 						UNION ALL
								 					SELECT ZQNM FROM RWA.RWA_WSIB_ABS_INVEST_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
													)
																																										--从配置表排除资产证券化的债券内码
										 AND T1.DATANO = p_data_dt_str																	--债券信息表,获取有效的债券信息
								GROUP BY T4.BONDPUBLISHCOUNTRY, T4.MARKETSCATEGORY
		)
    SELECT 			--DISTINCT
        				 TO_DATE(p_data_dt_str,'yyyyMMdd')     																	AS DataDate            		--数据日期
        				,p_data_dt_str                         																	AS DataNo              		--数据流水号
        				,CASE WHEN T1.MARKETSCATEGORY = '01' THEN T1.COUNTRYCODE || 'ZYZF'																--境外主权国家或经济实体区域的中央政府
                			WHEN T1.MARKETSCATEGORY = '02' THEN T1.COUNTRYCODE || 'ZYYH'																--境外中央银行
                			ELSE T1.COUNTRYCODE || 'BMST'																																--境外国家或地区注册的公共部门实体
                 END																																		AS ClientID            		--参与主体ID            默认 中国中央政府
        				,CASE WHEN T1.MARKETSCATEGORY = '01' THEN T1.COUNTRYCODE || 'ZYZF'
                			WHEN T1.MARKETSCATEGORY = '02' THEN T1.COUNTRYCODE || 'ZYYH'
                			ELSE T1.COUNTRYCODE || 'BMST'
                 END																																		AS SourceClientID      		--源参与主体ID
        				,'TZ'                                 																	AS SSysID              		--源系统ID
        				,CASE WHEN T1.MARKETSCATEGORY = '01' THEN T1.COUNTRYCODE || '中央政府'														--境外主权国家或经济实体区域的中央政府
                			WHEN T1.MARKETSCATEGORY = '02' THEN T1.COUNTRYCODE || '中央银行'														--境外中央银行
                			ELSE T1.COUNTRYCODE || '公共部门实体'																												--境外国家或地区注册的公共部门实体
                 END																																		AS ClientName          		--参与主体名称          默认 中国中央政府
        				,T1.ORGID								              																	AS SOrgID              		--源机构ID
        				,T2.ORGNAME																															AS SOrgName            		--源机构名称
        				,T2.SORTNO																															AS OrgSortNo           		--所属机构排序号
        				,T1.ORGID									             																	AS OrgID               		--所属机构ID
        				,T2.ORGNAME																															AS OrgName             		--所属机构名称
        				,'999999'														  																	AS IndustryID          		--所属行业代码          默认 999999-未知
        				,'未知'											          																	AS IndustryName        		--所属行业名称          默认 999999-未知
        				,'01'																																		AS ClientType          		--参与主体大类          默认 01-主权
        				,CASE WHEN T1.MARKETSCATEGORY = '01' THEN '0102'																									--境外主权国家或经济实体区域的中央政府
                			WHEN T1.MARKETSCATEGORY = '02' THEN '0104'																									--境外中央银行
                			ELSE '0107'																																									--境外国家或地区注册的公共部门实体
                 END																																		AS ClientSubType       		--参与主体小类          默认 0101-中国中央政府
        				,'02'	                                 																	AS RegistState         		--注册国家或地区        默认 02-境外
        				,T3.RATINGRESULT                       																	AS RCERating           		--境外注册地外部评级
        				,'01'                                  																	AS RCERAgency          		--境外注册地外部评级机构
        				,T1.COUNTRYCODE || 'ZZJGDM'				     																	AS OrganizationCode    		--组织机构代码
        				,'0'	                                																	AS ConsolidatedSCFlag  		--是否并表子公司
        				,'0'                                  																	AS SLClientFlag        		--专业贷款客户标识
        				,''	                                  																	AS SLClientType        		--专业贷款客户类型
        				,CASE WHEN T1.MARKETSCATEGORY = '01' THEN '020101'																								--境外主权国家或经济实体区域的中央政府
                			WHEN T1.MARKETSCATEGORY = '02' THEN '020102'																								--境外中央银行
                			ELSE '020103'																																								--境外国家或地区注册的公共部门实体
                 END		                              																	AS ExpoCategoryIRB     		--内评法暴露类别        默认 020101-中央政府
        				,''				                            																	AS ModelID             		--模型ID
        				,''				                            																	AS ModelIRating        		--模型内部评级
        				,NULL                                 																	AS ModelPD             		--模型违约概率
        				,''								                    																	AS IRating             		--内部评级
        				,NULL	                                																	AS PD                  		--违约概率
        				,'0'																																		AS DefaultFlag         		--违约标识
        				,'0'											            																	AS NewDefaultFlag      		--新增违约标识
        				,''																     																	AS DefaultDate         		--违约时点
        				,T3.RATINGRESULT									     																	AS ClientERating       		--参与主体外部评级
        				,'0'                                  																	AS CCPFlag             		--中央交易对手标识
        				,'0'                                   																	AS QualCCPFlag         		--是否合格中央交易对手
        				,'0'                                   																	AS ClearMemberFlag     		--清算会员标识
        				,'00'									                																	AS CompanySize         		--企业规模
        				,'0'											            																	AS SSMBFlag            		--标准小微企业标识
        				,'0'											            																	AS SSMBFlagSTD         		--权重法标准小微企业标识
        				,NULL							                     																	AS AnnualSale          		--公司客户年销售额
        				,T1.COUNTRYCODE																													AS CountryCode            --注册国家代码
        				,''																																			AS MSMBFlag								--工信部微小企业标识

    FROM				TMP_BND_ORG T1
    LEFT JOIN		RWA.ORG_INFO T2
    ON					T1.ORGID = T2.ORGID
    LEFT JOIN		RWA.RWA_WP_COUNTRYRATING T3
    ON					T1.COUNTRYCODE = T3.COUNTRYCODE
    AND					T3.ISINUSE = '1'
	  ;

		COMMIT;

		--毛主席像章
    INSERT INTO RWA_DEV.RWA_TZ_CLIENT(
         DataDate                   --数据日期
        ,DataNo                     --数据流水号
        ,ClientID                   --参与主体ID
        ,SourceClientID             --源参与主体ID
        ,SSysID                     --源系统ID
        ,ClientName                 --参与主体名称
        ,SOrgID                     --源机构ID
        ,SOrgName                   --源机构名称
        ,OrgSortNo                  --所属机构排序号
        ,OrgID                      --所属机构ID
        ,OrgName                    --所属机构名称
        ,IndustryID                 --所属行业代码
        ,IndustryName               --所属行业名称
        ,ClientType                 --参与主体大类
        ,ClientSubType              --参与主体小类
        ,RegistState                --注册国家或地区
        ,RCERating                  --境外注册地外部评级
        ,RCERAgency                 --境外注册地外部评级机构
        ,OrganizationCode           --组织机构代码
        ,ConsolidatedSCFlag         --是否并表子公司
        ,SLClientFlag               --专业贷款客户标识
        ,SLClientType               --专业贷款客户类型
        ,ExpoCategoryIRB            --内评法暴露类别
        ,ModelID                    --模型ID
        ,ModelIRating               --模型内部评级
        ,ModelPD                    --模型违约概率
        ,IRating                    --内部评级
        ,PD                         --违约概率
        ,DefaultFlag                --违约标识
        ,NewDefaultFlag             --新增违约标识
        ,DefaultDate                --违约时点
        ,ClientERating              --参与主体外部评级
        ,CCPFlag                    --中央交易对手标识
        ,QualCCPFlag                --是否合格中央交易对手
        ,ClearMemberFlag            --清算会员标识
        ,CompanySize                --企业规模
        ,SSMBFlag                   --标准小微企业标识
        ,SSMBFLAGSTD         				--权重法标准小微企业标识
        ,AnnualSale                 --公司客户年销售额
        ,CountryCode                --注册国家代码
        ,MSMBFlag										--工信部微小企业标识
    )
  	WITH TEMP_BND_BOOK AS (
    										SELECT BOND_ID,
												       INITIAL_COST,
												       INT_ADJUST,
												       MKT_VALUE_CHANGE,
												       RECEIVABLE_INT,
												       ACCOUNTABLE_INT
												  FROM (SELECT BOND_ID,
												               INITIAL_COST,
												               INT_ADJUST,
												               MKT_VALUE_CHANGE,
												               RECEIVABLE_INT,
												               ACCOUNTABLE_INT,
												               ROW_NUMBER() OVER(PARTITION BY BOND_ID ORDER BY SORT_SEQ DESC) RM
												          FROM FNS_BND_BOOK_B
												         WHERE AS_OF_DATE <= p_data_dt_str
												         	 AND BOND_ID = 'B200801010095'
												           AND DATANO = p_data_dt_str)
												 WHERE RM = 1
												   AND NVL(INITIAL_COST, 0) + NVL(INT_ADJUST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0
		)
		SELECT 			--DISTINCT
        				 TO_DATE(p_data_dt_str,'yyyyMMdd')     																	AS DataDate            		--数据日期
        				,p_data_dt_str                         																	AS DataNo              		--数据流水号
        				,'MZXXZ'																																AS ClientID            		--参与主体ID            默认 MZXXZ
        				,'MZXXZ'																																AS SourceClientID      		--源参与主体ID
        				,'TZ'                                 																	AS SSysID              		--源系统ID
        				,'毛主席像'																															AS ClientName          		--参与主体名称          默认 毛主席像
        				,T3.ORGID								              																	AS SOrgID              		--源机构ID
        				,T3.ORGNAME																															AS SOrgName            		--源机构名称
        				,T3.SORTNO																															AS OrgSortNo           		--所属机构排序号
        				,T3.ORGID									             																	AS OrgID               		--所属机构ID
        				,T3.ORGNAME																															AS OrgName             		--所属机构名称
        				,'J66'															  																	AS IndustryID          		--所属行业代码          默认 J66-货币金融服务
        				,'货币金融服务'							          																	AS IndustryName        		--所属行业名称          默认 J66-货币金融服务
        				,'02'																																		AS ClientType          		--参与主体大类          默认 02-金融机构
        				,'0205'																																	AS ClientSubType       		--参与主体小类          默认 0205-中国其他金融机构
        				,'01'	                                 																	AS RegistState         		--注册国家或地区        默认 01-境内
        				,''							                       																	AS RCERating           		--境外注册地外部评级
        				,'01'                                  																	AS RCERAgency          		--境外注册地外部评级机构
        				,'MZXXZZZJGDM'										     																	AS OrganizationCode    		--组织机构代码
        				,'0'	                                																	AS ConsolidatedSCFlag  		--是否并表子公司
        				,'0'                                  																	AS SLClientFlag        		--专业贷款客户标识
        				,''	                                  																	AS SLClientType        		--专业贷款客户类型
        				,'020202'                              																	AS ExpoCategoryIRB     		--内评法暴露类别        默认 020202-非银行类金融机构
        				,''				                            																	AS ModelID             		--模型ID
        				,''				                            																	AS ModelIRating        		--模型内部评级
        				,NULL                                 																	AS ModelPD             		--模型违约概率
        				,''								                    																	AS IRating             		--内部评级
        				,NULL	                                																	AS PD                  		--违约概率
        				,'0'																																		AS DefaultFlag         		--违约标识
        				,'0'											            																	AS NewDefaultFlag      		--新增违约标识
        				,''																     																	AS DefaultDate         		--违约时点
        				,''																     																	AS ClientERating       		--参与主体外部评级
        				,'0'                                  																	AS CCPFlag             		--中央交易对手标识
        				,'0'                                   																	AS QualCCPFlag         		--是否合格中央交易对手
        				,'0'                                   																	AS ClearMemberFlag     		--清算会员标识
        				,'00'									                																	AS CompanySize         		--企业规模
        				,'0'											            																	AS SSMBFlag            		--标准小微企业标识
        				,'0'											            																	AS SSMBFlagSTD         		--权重法标准小微企业标识
        				,NULL							                     																	AS AnnualSale          		--公司客户年销售额
        				,'CHN'																																	AS CountryCode            --注册国家代码
        				,''																																			AS MSMBFlag								--工信部微小企业标识

    FROM				RWA_DEV.FNS_BND_INFO_B T1
		INNER JOIN	TEMP_BND_BOOK T2
		ON 					T1.BOND_ID = T2.BOND_ID
		LEFT JOIN		RWA.ORG_INFO T3																            --RWA机构表
		ON					T1.DEPARTMENT = T3.ORGID
    WHERE 			(T1.ASSET_CLASS = '20' OR
								 (T1.ASSET_CLASS = '40' AND T1.BOND_TYPE1 NOT IN ('091','099')) OR
								 (T1.ASSET_CLASS = '40' AND T1.BOND_TYPE1 IN ('091','099') AND T1.CLOSED = '1')
								)
	  																																--通过资产分类来确定债券还是应收款投资。
	  																																--40 可供出售类资产
		--进入合规数据集的债券投资范围，需要排除“债券分类”=‘资产支持证券’的记录。因为这一类单独通过补录模板获取数据。不要算两遍
		AND 				T1.BOND_TYPE1 <> '060'
		AND					T1.BOND_ID NOT IN
								(SELECT ZQNM FROM RWA.RWA_WSIB_ABS_ISSUE_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
								 UNION ALL
								 SELECT ZQNM FROM RWA.RWA_WSIB_ABS_INVEST_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
								)
																																		--从配置表排除资产证券化的债券内码
		AND					T1.BOND_ID = 'B200801010095'												--毛主席像章
		AND 				T1.DATANO = p_data_dt_str														--债券信息表,获取有效的债券信息
	  ;

		COMMIT;

		--货币基金押品发行人
    INSERT INTO RWA_DEV.RWA_TZ_CLIENT(
         DataDate                   --数据日期
        ,DataNo                     --数据流水号
        ,ClientID                   --参与主体ID
        ,SourceClientID             --源参与主体ID
        ,SSysID                     --源系统ID
        ,ClientName                 --参与主体名称
        ,SOrgID                     --源机构ID
        ,SOrgName                   --源机构名称
        ,OrgSortNo                  --所属机构排序号
        ,OrgID                      --所属机构ID
        ,OrgName                    --所属机构名称
        ,IndustryID                 --所属行业代码
        ,IndustryName               --所属行业名称
        ,ClientType                 --参与主体大类
        ,ClientSubType              --参与主体小类
        ,RegistState                --注册国家或地区
        ,RCERating                  --境外注册地外部评级
        ,RCERAgency                 --境外注册地外部评级机构
        ,OrganizationCode           --组织机构代码
        ,ConsolidatedSCFlag         --是否并表子公司
        ,SLClientFlag               --专业贷款客户标识
        ,SLClientType               --专业贷款客户类型
        ,ExpoCategoryIRB            --内评法暴露类别
        ,ModelID                    --模型ID
        ,ModelIRating               --模型内部评级
        ,ModelPD                    --模型违约概率
        ,IRating                    --内部评级
        ,PD                         --违约概率
        ,DefaultFlag                --违约标识
        ,NewDefaultFlag             --新增违约标识
        ,DefaultDate                --违约时点
        ,ClientERating              --参与主体外部评级
        ,CCPFlag                    --中央交易对手标识
        ,QualCCPFlag                --是否合格中央交易对手
        ,ClearMemberFlag            --清算会员标识
        ,CompanySize                --企业规模
        ,SSMBFlag                   --标准小微企业标识
        ,SSMBFLAGSTD         				--权重法标准小微企业标识
        ,AnnualSale                 --公司客户年销售额
        ,CountryCode                --注册国家代码
        ,MSMBFlag										--工信部微小企业标识
    )
		SELECT 			--DISTINCT
        				 TO_DATE(p_data_dt_str,'yyyyMMdd')     																	AS DataDate            		--数据日期
        				,p_data_dt_str                         																	AS DataNo              		--数据流水号
        				,T1.CUSTID1																															AS ClientID            		--参与主体ID
        				,T1.CUSTID1																															AS SourceClientID      		--源参与主体ID
        				,'TZ'                                 																	AS SSysID              		--源系统ID
        				,T1.GUARANTORNAME																												AS ClientName          		--参与主体名称
        				,T1.BELONGORGCODE				              																	AS SOrgID              		--源机构ID
        				,T3.ORGNAME																															AS SOrgName            		--源机构名称
        				,T3.SORTNO																															AS OrgSortNo           		--所属机构排序号
        				,T1.BELONGORGCODE					             																	AS OrgID               		--所属机构ID
        				,T3.ORGNAME																															AS OrgName             		--所属机构名称
        				,'J66'															  																	AS IndustryID          		--所属行业代码          默认 J66-货币金融服务
        				,'货币金融服务'							          																	AS IndustryName        		--所属行业名称          默认 J66-货币金融服务
        				,SUBSTR(T1.GUARANTORCATEGORY,1,2)																				AS ClientType          		--参与主体大类
        				,T1.GUARANTORCATEGORY																										AS ClientSubType       		--参与主体小类
        				,CASE WHEN T1.GUARANTORCOUNTRYCODE = 'CHN' THEN '01'
                 ELSE '02'
                 END	                                 																	AS RegistState         		--注册国家或地区
        				,T5.RATINGRESULT                       																	AS RCERating           		--境外注册地外部评级
        				,'01'                                  																	AS RCERAgency          		--境外注册地外部评级机构
        				,''																     																	AS OrganizationCode    		--组织机构代码
        				,'0'	                                																	AS ConsolidatedSCFlag  		--是否并表子公司
        				,'0'                                  																	AS SLClientFlag        		--专业贷款客户标识
        				,''	                                  																	AS SLClientType        		--专业贷款客户类型
        				,''			                              																	AS ExpoCategoryIRB     		--内评法暴露类别
        				,''				                            																	AS ModelID             		--模型ID
        				,''				                            																	AS ModelIRating        		--模型内部评级
        				,NULL                                 																	AS ModelPD             		--模型违约概率
        				,''								                    																	AS IRating             		--内部评级
        				,NULL	                                																	AS PD                  		--违约概率
        				,'0'																																		AS DefaultFlag         		--违约标识
        				,'0'											            																	AS NewDefaultFlag      		--新增违约标识
        				,''																     																	AS DefaultDate         		--违约时点
        				,''																     																	AS ClientERating       		--参与主体外部评级
        				,'0'                                  																	AS CCPFlag             		--中央交易对手标识
        				,'0'                                   																	AS QualCCPFlag         		--是否合格中央交易对手
        				,'0'                                   																	AS ClearMemberFlag     		--清算会员标识
        				,'00'									                																	AS CompanySize         		--企业规模
        				,'0'											            																	AS SSMBFlag            		--标准小微企业标识
        				,'0'											            																	AS SSMBFlagSTD         		--权重法标准小微企业标识
        				,NULL							                     																	AS AnnualSale          		--公司客户年销售额
        				,'CHN'																																	AS CountryCode            --注册国家代码
        				,''																																			AS MSMBFlag								--工信部微小企业标识

    FROM				RWA.RWA_WS_BONDTRADE_MF T1	 																--货币基金债券投资补录表
		INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT T2
    ON          T1.SUPPORGID = T2.ORGID
    AND         T2.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND         T2.SUPPTMPLID = 'M-0071'
    AND         T2.SUBMITFLAG = '1'
    LEFT JOIN		RWA.ORG_INFO T3
    ON					T1.BELONGORGCODE = T3.ORGID
    LEFT JOIN		RWA.RWA_WP_COUNTRYRATING T5
    ON 					T1.GUARANTORCOUNTRYCODE = T5.COUNTRYCODE
    AND					T5.ISINUSE = '1'
		WHERE 			T1.CUSTID1 IS NOT NULL
		AND					T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
	  ;

		COMMIT;

		dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TZ_CLIENT',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_TZ_CLIENT;
    --DBMS_OUTPUT.PUT_LINE('RWA_DEV.RWA_TZ_CLIENT，中插入数量为：' || v_count || '条');
    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功' || '-' || v_count;

    commit;
    --定义异常
    EXCEPTION WHEN OTHERS THEN
    		--DBMS_OUTPUT.PUT_LINE('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '理财资管系统-参与主体('|| v_pro_name ||')ETL转换失败！'|| sqlerrm;
    		RETURN;

END PRO_RWA_TZ_CLIENT_0808;
/

