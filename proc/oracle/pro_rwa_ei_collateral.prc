CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_COLLATERAL(
			 											P_DATA_DT_STR	IN	VARCHAR2,		--��������
       											P_PO_RTNCODE	OUT	VARCHAR2,		--���ر��
														P_PO_RTNMSG		OUT	VARCHAR2		--��������
)
  /*
    �洢��������:RWA_DEV.PRO_RWA_EI_COLLATERAL
    ʵ�ֹ���:���ܵ���ѺƷ��,�������е���ѺƷ��Ϣ
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�
    ��  ��  :V1.0.0
    ��д��  :SHUXD
    ��дʱ��:2014-06-01
    ��  λ	:�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1	:RWA_DEV.RWA_XD_COLLATERAL|�Ŵ�����ѺƷ��
    Դ  ��2	:RWA_DEV.RWA_PJ_COLLATERAL|Ʊ�ݵ���ѺƷ��
    Դ  ��3	:RWA_DEV.RWA_TZ_COLLATERAL|Ͷ�ʵ���ѺƷ��
    Դ  ��4	:RWA_DEV.RWA_LC_COLLATERAL|��Ƶ���ѺƷ��
    Դ  ��5	:RWA_DEV.RWA_HG_COLLATERAL|�ع�����ѺƷ��
    Դ  ��6	:RWA_DEV.RWA_ABS_ISSURE_COLLATERAL|�ʲ�֤ȯ������ѺƷ��
    Ŀ���	:RWA_DEV.RWA_EI_COLLATERAL|���ܵ���ѺƷ��
    ������	:��
    �����¼(�޸���|�޸�ʱ��|�޸�����)��
  */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_COLLATERAL';
  --�����ж�ֵ����
  v_count INTEGER;
  --�����쳣����
  v_raise EXCEPTION;
  --������ʱ����
  v_tabname VARCHAR2(200);
  --���崴�����
  v_create VARCHAR2(1000) ;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    BEGIN
    --ɾ�������ڵķ���������˴洢�����ǵ�һ�����׳��쳣�����Ժ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_COLLATERAL DROP PARTITION COLLATERAL' || p_data_dt_str;

    COMMIT;

    EXCEPTION
      WHEN OTHERS THEN
        IF (SQLCODE <> '-2149') THEN
          --�״η���truncate�����2149�쳣
         p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '���ܵ���ѺƷ��('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
        END IF;
    END;

    --����һ����ǰ�����µķ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_COLLATERAL ADD PARTITION COLLATERAL' || p_data_dt_str || ' VALUES(TO_DATE('|| p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;

    /*�����Ŵ��ĵ���ѺƷ��Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_COLLATERAL(
								 DATADATE          										  --��������
								,DATANO                 								--������ˮ��
								,COLLATERALID           								--����ѺƷID
								,SSYSID                 								--ԴϵͳID
								,SGUARCONTRACTID        								--Դ������ͬID
								,SCOLLATERALID          								--Դ����ѺƷID
								,COLLATERALNAME         								--����ѺƷ����
								,ISSUERID               								--������ID
								,PROVIDERID             								--�ṩ��ID
								,CREDITRISKDATATYPE     								--���÷�����������
								,GUARANTEEWAY            								--������ʽ
								,SOURCECOLTYPE      										--Դ����ѺƷ����
								,SOURCECOLSUBTYPE       								--Դ����ѺƷС��
								,SPECPURPBONDFLAG       								--�Ƿ�Ϊ�չ��������в�����������е�ծȯ
								,QUALFLAGSTD            								--Ȩ�ط��ϸ��ʶ
								,QUALFLAGFIRB           								--�����������ϸ��ʶ
								,COLLATERALTYPESTD      								--Ȩ�ط�����ѺƷ����
								,COLLATERALSDVSSTD      								--Ȩ�ط�����ѺƷϸ��
								,COLLATERALTYPEIRB      								--����������ѺƷ����
								,COLLATERALAMOUNT        								--��Ѻ�ܶ�
								,CURRENCY               								--����
								,STARTDATE              								--��ʼ����
								,DUEDATE                								--��������
								,ORIGINALMATURITY       								--ԭʼ����
								,RESIDUALM              								--ʣ������
								,INTEHAIRCUTSFLAG       								--���й����ۿ�ϵ����ʶ
								,INTERNALHC             								--�ڲ��ۿ�ϵ��
								,FCTYPE                 								--������ѺƷ����
								,ABSFLAG                								--�ʲ�֤ȯ����ʶ
								,RATINGDURATIONTYPE     								--������������
								,FCISSUERATING          								--������ѺƷ���еȼ�
								,FCISSUERTYPE           								--������ѺƷ���������
								,FCISSUERSTATE          								--������ѺƷ������ע�����
								,FCRESIDUALM            								--������ѺƷʣ������
								,REVAFREQUENCY          								--�ع�Ƶ��
								,GROUPID                								--������
								,RCERating                              --�����˾���ע����ⲿ����
		)
		SELECT
								 DATADATE          																							    AS DATADATE          	  --��������
								,DATANO             																						    AS DATANO              	--������ˮ��
								,COLLATERALID       																						    AS COLLATERALID        	--����ѺƷID
								,SSYSID             																								AS SSYSID              	--ԴϵͳID
								,SGUARCONTRACTID    																							  AS SGUARCONTRACTID     	--Դ������ͬID
								,SCOLLATERALID      																						    AS SCOLLATERALID       	--Դ����ѺƷID
								,COLLATERALNAME     																							  AS COLLATERALNAME      	--����ѺƷ����
								,ISSUERID           														                    AS ISSUERID            	--������ID
								,PROVIDERID         																					      AS PROVIDERID          	--�ṩ��ID
								,CREDITRISKDATATYPE 																						    AS CREDITRISKDATATYPE  	--���÷�����������
								,GUARANTEEWAY       																								AS GUARANTEEWAY      	  --������ʽ
								,SOURCECOLTYPE      																								AS SOURCECOLTYPE     	  --Դ����ѺƷ����
								,SOURCECOLSUBTYPE   																								AS SOURCECOLSUBTYPE    	--Դ����ѺƷС��
								,SPECPURPBONDFLAG   																								AS SPECPURPBONDFLAG    	--�Ƿ�Ϊ�չ��������в�����������е�ծȯ
								,QUALFLAGSTD        																								AS QUALFLAGSTD         	--Ȩ�ط��ϸ��ʶ
								,QUALFLAGFIRB       																	              AS QUALFLAGFIRB        	--�����������ϸ��ʶ
								,COLLATERALTYPESTD  																		            AS COLLATERALTYPESTD   	--Ȩ�ط�����ѺƷ����
								,COLLATERALSDVSSTD  																		            AS COLLATERALSDVSSTD   	--Ȩ�ط�����ѺƷϸ��
								,COLLATERALTYPEIRB  																								AS COLLATERALTYPEIRB   	--����������ѺƷ����
								,COLLATERALAMOUNT   																								AS COLLATERALAMOUNT     --��Ѻ�ܶ�                              -
								,CURRENCY           																								AS CURRENCY            	--����
								,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')          		AS STARTDATE           	--��ʼ����
								,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')            		AS DUEDATE             	--��������
								,ORIGINALMATURITY                                                   AS ORIGINALMATURITY    	--ԭʼ����
								,RESIDUALM                                                          AS RESIDUALM           	--ʣ������
								,INTEHAIRCUTSFLAG   																			          AS INTEHAIRCUTSFLAG    	--���й����ۿ�ϵ����ʶ
								,INTERNALHC         																							  AS INTERNALHC          	--�ڲ��ۿ�ϵ��
								,FCTYPE             																								AS FCTYPE              	--������ѺƷ����
								,ABSFLAG            																							  AS ABSFLAG             	--�ʲ�֤ȯ����ʶ
								,RATINGDURATIONTYPE 																							  AS RATINGDURATIONTYPE  	--������������
								,FCISSUERATING      																				        AS FCISSUERATING       	--������ѺƷ���еȼ�
								,FCISSUERTYPE                                    		                AS FCISSUERTYPE        	--������ѺƷ���������
								,FCISSUERSTATE                            													AS FCISSUERSTATE       	--������ѺƷ������ע�����
								,FCRESIDUALM                                                        AS FCRESIDUALM         	--������ѺƷʣ������
								,REVAFREQUENCY      																	 				      AS REVAFREQUENCY       	--�ع�Ƶ��
								,GROUPID                                                   					AS GROUPID             	--������
								,RCERating                                                          AS RCERating            --�����˾���ע����ⲿ����
		FROM   			RWA_DEV.RWA_XD_COLLATERAL
		WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
		;
		COMMIT;


    /*����Ʊ�ݵĵ���ѺƷ��Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_COLLATERAL(
								 DATADATE          										  --��������
								,DATANO                 								--������ˮ��
								,COLLATERALID           								--����ѺƷID
								,SSYSID                 								--ԴϵͳID
								,SGUARCONTRACTID        								--Դ������ͬID
								,SCOLLATERALID          								--Դ����ѺƷID
								,COLLATERALNAME         								--����ѺƷ����
								,ISSUERID               								--������ID
								,PROVIDERID             								--�ṩ��ID
								,CREDITRISKDATATYPE     								--���÷�����������
								,GUARANTEEWAY            								--������ʽ
								,SOURCECOLTYPE      										--Դ����ѺƷ����
								,SOURCECOLSUBTYPE       								--Դ����ѺƷС��
								,SPECPURPBONDFLAG       								--�Ƿ�Ϊ�չ��������в�����������е�ծȯ
								,QUALFLAGSTD            								--Ȩ�ط��ϸ��ʶ
								,QUALFLAGFIRB           								--�����������ϸ��ʶ
								,COLLATERALTYPESTD      								--Ȩ�ط�����ѺƷ����
								,COLLATERALSDVSSTD      								--Ȩ�ط�����ѺƷϸ��
								,COLLATERALTYPEIRB      								--����������ѺƷ����
								,COLLATERALAMOUNT        								--��Ѻ�ܶ�
								,CURRENCY               								--����
								,STARTDATE              								--��ʼ����
								,DUEDATE                								--��������
								,ORIGINALMATURITY       								--ԭʼ����
								,RESIDUALM              								--ʣ������
								,INTEHAIRCUTSFLAG       								--���й����ۿ�ϵ����ʶ
								,INTERNALHC             								--�ڲ��ۿ�ϵ��
								,FCTYPE                 								--������ѺƷ����
								,ABSFLAG                								--�ʲ�֤ȯ����ʶ
								,RATINGDURATIONTYPE     								--������������
								,FCISSUERATING          								--������ѺƷ���еȼ�
								,FCISSUERTYPE           								--������ѺƷ���������
								,FCISSUERSTATE          								--������ѺƷ������ע�����
								,FCRESIDUALM            								--������ѺƷʣ������
								,REVAFREQUENCY          								--�ع�Ƶ��
								,GROUPID                								--������
								,RCERating                              --�����˾���ע����ⲿ����
		)
		SELECT
								 DATADATE          																							    AS DATADATE          	  --��������
								,DATANO             																						    AS DATANO              	--������ˮ��
								,COLLATERALID       																						    AS COLLATERALID        	--����ѺƷID
								,SSYSID             																								AS SSYSID              	--ԴϵͳID
								,SGUARCONTRACTID    																							  AS SGUARCONTRACTID     	--Դ������ͬID
								,SCOLLATERALID      																						    AS SCOLLATERALID       	--Դ����ѺƷID
								,COLLATERALNAME     																							  AS COLLATERALNAME      	--����ѺƷ����
								,ISSUERID           														                    AS ISSUERID            	--������ID
								,PROVIDERID         																					      AS PROVIDERID          	--�ṩ��ID
								,CREDITRISKDATATYPE 																						    AS CREDITRISKDATATYPE  	--���÷�����������
								,GUARANTEEWAY       																								AS GUARANTEEWAY      	  --������ʽ
								,SOURCECOLTYPE      																								AS SOURCECOLTYPE     	  --Դ����ѺƷ����
								,SOURCECOLSUBTYPE   																								AS SOURCECOLSUBTYPE    	--Դ����ѺƷС��
								,SPECPURPBONDFLAG   																								AS SPECPURPBONDFLAG    	--�Ƿ�Ϊ�չ��������в�����������е�ծȯ
								,QUALFLAGSTD        																								AS QUALFLAGSTD         	--Ȩ�ط��ϸ��ʶ
								,QUALFLAGFIRB       																	              AS QUALFLAGFIRB        	--�����������ϸ��ʶ
								,COLLATERALTYPESTD  																		            AS COLLATERALTYPESTD   	--Ȩ�ط�����ѺƷ����
								,COLLATERALSDVSSTD  																		            AS COLLATERALSDVSSTD   	--Ȩ�ط�����ѺƷϸ��
								,COLLATERALTYPEIRB  																								AS COLLATERALTYPEIRB   	--����������ѺƷ����
								,COLLATERALAMOUNT   																								AS COLLATERALAMOUNT     --��Ѻ�ܶ�                              -
								,CURRENCY           																								AS CURRENCY            	--����
								,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')          		AS STARTDATE           	--��ʼ����
								,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')            		AS DUEDATE             	--��������
								,ORIGINALMATURITY                                                   AS ORIGINALMATURITY    	--ԭʼ����
								,RESIDUALM                                                          AS RESIDUALM           	--ʣ������
								,INTEHAIRCUTSFLAG   																			          AS INTEHAIRCUTSFLAG    	--���й����ۿ�ϵ����ʶ
								,INTERNALHC         																							  AS INTERNALHC          	--�ڲ��ۿ�ϵ��
								,FCTYPE             																								AS FCTYPE              	--������ѺƷ����
								,ABSFLAG            																							  AS ABSFLAG             	--�ʲ�֤ȯ����ʶ
								,RATINGDURATIONTYPE 																							  AS RATINGDURATIONTYPE  	--������������
								,FCISSUERATING      																				        AS FCISSUERATING       	--������ѺƷ���еȼ�
								,FCISSUERTYPE                                    		                AS FCISSUERTYPE        	--������ѺƷ���������
								,FCISSUERSTATE                            													AS FCISSUERSTATE       	--������ѺƷ������ע�����
								,FCRESIDUALM                                                        AS FCRESIDUALM         	--������ѺƷʣ������
								,REVAFREQUENCY      																	 				      AS REVAFREQUENCY       	--�ع�Ƶ��
								,GROUPID                                                   					AS GROUPID             	--������
								,RCERating                                                          AS RCERating            --�����˾���ע����ⲿ����
		FROM   			RWA_DEV.RWA_PJ_COLLATERAL
		WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
		;
		COMMIT;


		/*������Ƶĵ���ѺƷ��Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_COLLATERAL(
								 DATADATE          										  --��������
								,DATANO                 								--������ˮ��
								,COLLATERALID           								--����ѺƷID
								,SSYSID                 								--ԴϵͳID
								,SGUARCONTRACTID        								--Դ������ͬID
								,SCOLLATERALID          								--Դ����ѺƷID
								,COLLATERALNAME         								--����ѺƷ����
								,ISSUERID               								--������ID
								,PROVIDERID             								--�ṩ��ID
								,CREDITRISKDATATYPE     								--���÷�����������
								,GUARANTEEWAY            								--������ʽ
								,SOURCECOLTYPE      										--Դ����ѺƷ����
								,SOURCECOLSUBTYPE       								--Դ����ѺƷС��
								,SPECPURPBONDFLAG       								--�Ƿ�Ϊ�չ��������в�����������е�ծȯ
								,QUALFLAGSTD            								--Ȩ�ط��ϸ��ʶ
								,QUALFLAGFIRB           								--�����������ϸ��ʶ
								,COLLATERALTYPESTD      								--Ȩ�ط�����ѺƷ����
								,COLLATERALSDVSSTD      								--Ȩ�ط�����ѺƷϸ��
								,COLLATERALTYPEIRB      								--����������ѺƷ����
								,COLLATERALAMOUNT        								--��Ѻ�ܶ�
								,CURRENCY               								--����
								,STARTDATE              								--��ʼ����
								,DUEDATE                								--��������
								,ORIGINALMATURITY       								--ԭʼ����
								,RESIDUALM              								--ʣ������
								,INTEHAIRCUTSFLAG       								--���й����ۿ�ϵ����ʶ
								,INTERNALHC             								--�ڲ��ۿ�ϵ��
								,FCTYPE                 								--������ѺƷ����
								,ABSFLAG                								--�ʲ�֤ȯ����ʶ
								,RATINGDURATIONTYPE     								--������������
								,FCISSUERATING          								--������ѺƷ���еȼ�
								,FCISSUERTYPE           								--������ѺƷ���������
								,FCISSUERSTATE          								--������ѺƷ������ע�����
								,FCRESIDUALM            								--������ѺƷʣ������
								,REVAFREQUENCY          								--�ع�Ƶ��
								,GROUPID                								--������
								,RCERating                              --�����˾���ע����ⲿ����
		)
		SELECT
								 DATADATE          																							    AS DATADATE          	  --��������
								,DATANO             																						    AS DATANO              	--������ˮ��
								,COLLATERALID       																						    AS COLLATERALID        	--����ѺƷID
								,SSYSID             																								AS SSYSID              	--ԴϵͳID
								,SGUARCONTRACTID    																							  AS SGUARCONTRACTID     	--Դ������ͬID
								,SCOLLATERALID      																						    AS SCOLLATERALID       	--Դ����ѺƷID
								,COLLATERALNAME     																							  AS COLLATERALNAME      	--����ѺƷ����
								,ISSUERID           														                    AS ISSUERID            	--������ID
								,PROVIDERID         																					      AS PROVIDERID          	--�ṩ��ID
								,CREDITRISKDATATYPE 																						    AS CREDITRISKDATATYPE  	--���÷�����������
								,GUARANTEEWAY       																								AS GUARANTEEWAY      	  --������ʽ
								,SOURCECOLTYPE      																								AS SOURCECOLTYPE     	  --Դ����ѺƷ����
								,SOURCECOLSUBTYPE   																								AS SOURCECOLSUBTYPE    	--Դ����ѺƷС��
								,SPECPURPBONDFLAG   																								AS SPECPURPBONDFLAG    	--�Ƿ�Ϊ�չ��������в�����������е�ծȯ
								,QUALFLAGSTD        																								AS QUALFLAGSTD         	--Ȩ�ط��ϸ��ʶ
								,QUALFLAGFIRB       																	              AS QUALFLAGFIRB        	--�����������ϸ��ʶ
								,COLLATERALTYPESTD  																		            AS COLLATERALTYPESTD   	--Ȩ�ط�����ѺƷ����
								,COLLATERALSDVSSTD  																		            AS COLLATERALSDVSSTD   	--Ȩ�ط�����ѺƷϸ��
								,COLLATERALTYPEIRB  																								AS COLLATERALTYPEIRB   	--����������ѺƷ����
								,COLLATERALAMOUNT   																								AS COLLATERALAMOUNT     --��Ѻ�ܶ�                              -
								,CURRENCY           																								AS CURRENCY            	--����
								,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')          		AS STARTDATE           	--��ʼ����
								,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')            		AS DUEDATE             	--��������
								,ORIGINALMATURITY                                                   AS ORIGINALMATURITY    	--ԭʼ����
								,RESIDUALM                                                          AS RESIDUALM           	--ʣ������
								,INTEHAIRCUTSFLAG   																			          AS INTEHAIRCUTSFLAG    	--���й����ۿ�ϵ����ʶ
								,INTERNALHC         																							  AS INTERNALHC          	--�ڲ��ۿ�ϵ��
								,FCTYPE             																								AS FCTYPE              	--������ѺƷ����
								,ABSFLAG            																							  AS ABSFLAG             	--�ʲ�֤ȯ����ʶ
								,RATINGDURATIONTYPE 																							  AS RATINGDURATIONTYPE  	--������������
								,FCISSUERATING      																				        AS FCISSUERATING       	--������ѺƷ���еȼ�
								,FCISSUERTYPE                                    		                AS FCISSUERTYPE        	--������ѺƷ���������
								,FCISSUERSTATE                            													AS FCISSUERSTATE       	--������ѺƷ������ע�����
								,FCRESIDUALM                                                        AS FCRESIDUALM         	--������ѺƷʣ������
								,REVAFREQUENCY      																	 				      AS REVAFREQUENCY       	--�ع�Ƶ��
								,GROUPID                                                   					AS GROUPID             	--������
								,RCERating                                                          AS RCERating            --�����˾���ע����ⲿ����
		FROM   			RWA_DEV.RWA_LC_COLLATERAL
		WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
		;
		COMMIT;


		/*����Ͷ�ʵĵ���ѺƷ��Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_COLLATERAL(
								 DATADATE          										  --��������
								,DATANO                 								--������ˮ��
								,COLLATERALID           								--����ѺƷID
								,SSYSID                 								--ԴϵͳID
								,SGUARCONTRACTID        								--Դ������ͬID
								,SCOLLATERALID          								--Դ����ѺƷID
								,COLLATERALNAME         								--����ѺƷ����
								,ISSUERID               								--������ID
								,PROVIDERID             								--�ṩ��ID
								,CREDITRISKDATATYPE     								--���÷�����������
								,GUARANTEEWAY            								--������ʽ
								,SOURCECOLTYPE      										--Դ����ѺƷ����
								,SOURCECOLSUBTYPE       								--Դ����ѺƷС��
								,SPECPURPBONDFLAG       								--�Ƿ�Ϊ�չ��������в�����������е�ծȯ
								,QUALFLAGSTD            								--Ȩ�ط��ϸ��ʶ
								,QUALFLAGFIRB           								--�����������ϸ��ʶ
								,COLLATERALTYPESTD      								--Ȩ�ط�����ѺƷ����
								,COLLATERALSDVSSTD      								--Ȩ�ط�����ѺƷϸ��
								,COLLATERALTYPEIRB      								--����������ѺƷ����
								,COLLATERALAMOUNT        								--��Ѻ�ܶ�
								,CURRENCY               								--����
								,STARTDATE              								--��ʼ����
								,DUEDATE                								--��������
								,ORIGINALMATURITY       								--ԭʼ����
								,RESIDUALM              								--ʣ������
								,INTEHAIRCUTSFLAG       								--���й����ۿ�ϵ����ʶ
								,INTERNALHC             								--�ڲ��ۿ�ϵ��
								,FCTYPE                 								--������ѺƷ����
								,ABSFLAG                								--�ʲ�֤ȯ����ʶ
								,RATINGDURATIONTYPE     								--������������
								,FCISSUERATING          								--������ѺƷ���еȼ�
								,FCISSUERTYPE           								--������ѺƷ���������
								,FCISSUERSTATE          								--������ѺƷ������ע�����
								,FCRESIDUALM            								--������ѺƷʣ������
								,REVAFREQUENCY          								--�ع�Ƶ��
								,GROUPID                								--������
								,RCERating                              --�����˾���ע����ⲿ����
		)
		SELECT
								 DATADATE          																							    AS DATADATE          	  --��������
								,DATANO             																						    AS DATANO              	--������ˮ��
								,COLLATERALID       																						    AS COLLATERALID        	--����ѺƷID
								,SSYSID             																								AS SSYSID              	--ԴϵͳID
								,SGUARCONTRACTID    																							  AS SGUARCONTRACTID     	--Դ������ͬID
								,SCOLLATERALID      																						    AS SCOLLATERALID       	--Դ����ѺƷID
								,COLLATERALNAME     																							  AS COLLATERALNAME      	--����ѺƷ����
								,ISSUERID           														                    AS ISSUERID            	--������ID
								,PROVIDERID         																					      AS PROVIDERID          	--�ṩ��ID
								,CREDITRISKDATATYPE 																						    AS CREDITRISKDATATYPE  	--���÷�����������
								,GUARANTEEWAY       																								AS GUARANTEEWAY      	  --������ʽ
								,SOURCECOLTYPE      																								AS SOURCECOLTYPE     	  --Դ����ѺƷ����
								,SOURCECOLSUBTYPE   																								AS SOURCECOLSUBTYPE    	--Դ����ѺƷС��
								,SPECPURPBONDFLAG   																								AS SPECPURPBONDFLAG    	--�Ƿ�Ϊ�չ��������в�����������е�ծȯ
								,QUALFLAGSTD        																								AS QUALFLAGSTD         	--Ȩ�ط��ϸ��ʶ
								,QUALFLAGFIRB       																	              AS QUALFLAGFIRB        	--�����������ϸ��ʶ
								,COLLATERALTYPESTD  																		            AS COLLATERALTYPESTD   	--Ȩ�ط�����ѺƷ����
								,COLLATERALSDVSSTD  																		            AS COLLATERALSDVSSTD   	--Ȩ�ط�����ѺƷϸ��
								,COLLATERALTYPEIRB  																								AS COLLATERALTYPEIRB   	--����������ѺƷ����
								,COLLATERALAMOUNT   																								AS COLLATERALAMOUNT     --��Ѻ�ܶ�                              -
								,CURRENCY           																								AS CURRENCY            	--����
								,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')          		AS STARTDATE           	--��ʼ����
								,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')            		AS DUEDATE             	--��������
								,ORIGINALMATURITY                                                   AS ORIGINALMATURITY    	--ԭʼ����
								,RESIDUALM                                                          AS RESIDUALM           	--ʣ������
								,INTEHAIRCUTSFLAG   																			          AS INTEHAIRCUTSFLAG    	--���й����ۿ�ϵ����ʶ
								,INTERNALHC         																							  AS INTERNALHC          	--�ڲ��ۿ�ϵ��
								,FCTYPE             																								AS FCTYPE              	--������ѺƷ����
								,ABSFLAG            																							  AS ABSFLAG             	--�ʲ�֤ȯ����ʶ
								,RATINGDURATIONTYPE 																							  AS RATINGDURATIONTYPE  	--������������
								,FCISSUERATING      																				        AS FCISSUERATING       	--������ѺƷ���еȼ�
								,FCISSUERTYPE                                    		                AS FCISSUERTYPE        	--������ѺƷ���������
								,FCISSUERSTATE                            													AS FCISSUERSTATE       	--������ѺƷ������ע�����
								,FCRESIDUALM                                                        AS FCRESIDUALM         	--������ѺƷʣ������
								,REVAFREQUENCY      																	 				      AS REVAFREQUENCY       	--�ع�Ƶ��
								,GROUPID                                                   					AS GROUPID             	--������
								,RCERating                                                          AS RCERating            --�����˾���ע����ⲿ����
		FROM   			RWA_DEV.RWA_TZ_COLLATERAL
		WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
		;
		COMMIT;

		/*����ع��ĵ���ѺƷ��Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_COLLATERAL(
								 DATADATE          										  --��������
								,DATANO                 								--������ˮ��
								,COLLATERALID           								--����ѺƷID
								,SSYSID                 								--ԴϵͳID
								,SGUARCONTRACTID        								--Դ������ͬID
								,SCOLLATERALID          								--Դ����ѺƷID
								,COLLATERALNAME         								--����ѺƷ����
								,ISSUERID               								--������ID
								,PROVIDERID             								--�ṩ��ID
								,CREDITRISKDATATYPE     								--���÷�����������
								,GUARANTEEWAY            								--������ʽ
								,SOURCECOLTYPE      										--Դ����ѺƷ����
								,SOURCECOLSUBTYPE       								--Դ����ѺƷС��
								,SPECPURPBONDFLAG       								--�Ƿ�Ϊ�չ��������в�����������е�ծȯ
								,QUALFLAGSTD            								--Ȩ�ط��ϸ��ʶ
								,QUALFLAGFIRB           								--�����������ϸ��ʶ
								,COLLATERALTYPESTD      								--Ȩ�ط�����ѺƷ����
								,COLLATERALSDVSSTD      								--Ȩ�ط�����ѺƷϸ��
								,COLLATERALTYPEIRB      								--����������ѺƷ����
								,COLLATERALAMOUNT        								--��Ѻ�ܶ�
								,CURRENCY               								--����
								,STARTDATE              								--��ʼ����
								,DUEDATE                								--��������
								,ORIGINALMATURITY       								--ԭʼ����
								,RESIDUALM              								--ʣ������
								,INTEHAIRCUTSFLAG       								--���й����ۿ�ϵ����ʶ
								,INTERNALHC             								--�ڲ��ۿ�ϵ��
								,FCTYPE                 								--������ѺƷ����
								,ABSFLAG                								--�ʲ�֤ȯ����ʶ
								,RATINGDURATIONTYPE     								--������������
								,FCISSUERATING          								--������ѺƷ���еȼ�
								,FCISSUERTYPE           								--������ѺƷ���������
								,FCISSUERSTATE          								--������ѺƷ������ע�����
								,FCRESIDUALM            								--������ѺƷʣ������
								,REVAFREQUENCY          								--�ع�Ƶ��
								,GROUPID                								--������
								,RCERating                              --�����˾���ע����ⲿ����
		)
		SELECT
								 DATADATE          																							    AS DATADATE          	  --��������
								,DATANO             																						    AS DATANO              	--������ˮ��
								,COLLATERALID       																						    AS COLLATERALID        	--����ѺƷID
								,SSYSID             																								AS SSYSID              	--ԴϵͳID
								,SGUARCONTRACTID    																							  AS SGUARCONTRACTID     	--Դ������ͬID
								,SCOLLATERALID      																						    AS SCOLLATERALID       	--Դ����ѺƷID
								,COLLATERALNAME     																							  AS COLLATERALNAME      	--����ѺƷ����
								,ISSUERID           														                    AS ISSUERID            	--������ID
								,PROVIDERID         																					      AS PROVIDERID          	--�ṩ��ID
								,CREDITRISKDATATYPE 																						    AS CREDITRISKDATATYPE  	--���÷�����������
								,GUARANTEEWAY       																								AS GUARANTEEWAY      	  --������ʽ
								,SOURCECOLTYPE      																								AS SOURCECOLTYPE     	  --Դ����ѺƷ����
								,SOURCECOLSUBTYPE   																								AS SOURCECOLSUBTYPE    	--Դ����ѺƷС��
								,SPECPURPBONDFLAG   																								AS SPECPURPBONDFLAG    	--�Ƿ�Ϊ�չ��������в�����������е�ծȯ
								,QUALFLAGSTD        																								AS QUALFLAGSTD         	--Ȩ�ط��ϸ��ʶ
								,QUALFLAGFIRB       																	              AS QUALFLAGFIRB        	--�����������ϸ��ʶ
								,COLLATERALTYPESTD  																		            AS COLLATERALTYPESTD   	--Ȩ�ط�����ѺƷ����
								,COLLATERALSDVSSTD  																		            AS COLLATERALSDVSSTD   	--Ȩ�ط�����ѺƷϸ��
								,COLLATERALTYPEIRB  																								AS COLLATERALTYPEIRB   	--����������ѺƷ����
								,COLLATERALAMOUNT   																								AS COLLATERALAMOUNT     --��Ѻ�ܶ�                              -
								,CURRENCY           																								AS CURRENCY            	--����
								,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')          		AS STARTDATE           	--��ʼ����
								,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')            		AS DUEDATE             	--��������
								,ORIGINALMATURITY                                                   AS ORIGINALMATURITY    	--ԭʼ����
								,RESIDUALM                                                          AS RESIDUALM           	--ʣ������
								,INTEHAIRCUTSFLAG   																			          AS INTEHAIRCUTSFLAG    	--���й����ۿ�ϵ����ʶ
								,INTERNALHC         																							  AS INTERNALHC          	--�ڲ��ۿ�ϵ��
								,FCTYPE             																								AS FCTYPE              	--������ѺƷ����
								,ABSFLAG            																							  AS ABSFLAG             	--�ʲ�֤ȯ����ʶ
								,RATINGDURATIONTYPE 																							  AS RATINGDURATIONTYPE  	--������������
								,FCISSUERATING      																				        AS FCISSUERATING       	--������ѺƷ���еȼ�
								,FCISSUERTYPE                                    		                AS FCISSUERTYPE        	--������ѺƷ���������
								,FCISSUERSTATE                            													AS FCISSUERSTATE       	--������ѺƷ������ע�����
								,FCRESIDUALM                                                        AS FCRESIDUALM         	--������ѺƷʣ������
								,REVAFREQUENCY      																	 				      AS REVAFREQUENCY       	--�ع�Ƶ��
								,GROUPID                                                   					AS GROUPID             	--������
								,RCERating                                                          AS RCERating            --�����˾���ע����ⲿ����
		FROM   			RWA_DEV.RWA_HG_COLLATERAL
		WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
		;
		COMMIT;

		/*�����ʲ�֤ȯ���ĵ���ѺƷ��Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_COLLATERAL(
								 DATADATE          										  --��������
								,DATANO                 								--������ˮ��
								,COLLATERALID           								--����ѺƷID
								,SSYSID                 								--ԴϵͳID
								,SGUARCONTRACTID        								--Դ������ͬID
								,SCOLLATERALID          								--Դ����ѺƷID
								,COLLATERALNAME         								--����ѺƷ����
								,ISSUERID               								--������ID
								,PROVIDERID             								--�ṩ��ID
								,CREDITRISKDATATYPE     								--���÷�����������
								,GUARANTEEWAY            								--������ʽ
								,SOURCECOLTYPE      										--Դ����ѺƷ����
								,SOURCECOLSUBTYPE       								--Դ����ѺƷС��
								,SPECPURPBONDFLAG       								--�Ƿ�Ϊ�չ��������в�����������е�ծȯ
								,QUALFLAGSTD            								--Ȩ�ط��ϸ��ʶ
								,QUALFLAGFIRB           								--�����������ϸ��ʶ
								,COLLATERALTYPESTD      								--Ȩ�ط�����ѺƷ����
								,COLLATERALSDVSSTD      								--Ȩ�ط�����ѺƷϸ��
								,COLLATERALTYPEIRB      								--����������ѺƷ����
								,COLLATERALAMOUNT        								--��Ѻ�ܶ�
								,CURRENCY               								--����
								,STARTDATE              								--��ʼ����
								,DUEDATE                								--��������
								,ORIGINALMATURITY       								--ԭʼ����
								,RESIDUALM              								--ʣ������
								,INTEHAIRCUTSFLAG       								--���й����ۿ�ϵ����ʶ
								,INTERNALHC             								--�ڲ��ۿ�ϵ��
								,FCTYPE                 								--������ѺƷ����
								,ABSFLAG                								--�ʲ�֤ȯ����ʶ
								,RATINGDURATIONTYPE     								--������������
								,FCISSUERATING          								--������ѺƷ���еȼ�
								,FCISSUERTYPE           								--������ѺƷ���������
								,FCISSUERSTATE          								--������ѺƷ������ע�����
								,FCRESIDUALM            								--������ѺƷʣ������
								,REVAFREQUENCY          								--�ع�Ƶ��
								,GROUPID                								--������
								,RCERating                              --�����˾���ע����ⲿ����
		)
		SELECT
								 DATADATE          																							    AS DATADATE          	  --��������
								,DATANO             																						    AS DATANO              	--������ˮ��
								,COLLATERALID       																						    AS COLLATERALID        	--����ѺƷID
								,SSYSID             																								AS SSYSID              	--ԴϵͳID
								,SGUARCONTRACTID    																							  AS SGUARCONTRACTID     	--Դ������ͬID
								,SCOLLATERALID      																						    AS SCOLLATERALID       	--Դ����ѺƷID
								,COLLATERALNAME     																							  AS COLLATERALNAME      	--����ѺƷ����
								,ISSUERID           														                    AS ISSUERID            	--������ID
								,PROVIDERID         																					      AS PROVIDERID          	--�ṩ��ID
								,CREDITRISKDATATYPE 																						    AS CREDITRISKDATATYPE  	--���÷�����������
								,GUARANTEEWAY       																								AS GUARANTEEWAY      	  --������ʽ
								,SOURCECOLTYPE      																								AS SOURCECOLTYPE     	  --Դ����ѺƷ����
								,SOURCECOLSUBTYPE   																								AS SOURCECOLSUBTYPE    	--Դ����ѺƷС��
								,SPECPURPBONDFLAG   																								AS SPECPURPBONDFLAG    	--�Ƿ�Ϊ�չ��������в�����������е�ծȯ
								,QUALFLAGSTD        																								AS QUALFLAGSTD         	--Ȩ�ط��ϸ��ʶ
								,QUALFLAGFIRB       																	              AS QUALFLAGFIRB        	--�����������ϸ��ʶ
								,COLLATERALTYPESTD  																		            AS COLLATERALTYPESTD   	--Ȩ�ط�����ѺƷ����
								,COLLATERALSDVSSTD  																		            AS COLLATERALSDVSSTD   	--Ȩ�ط�����ѺƷϸ��
								,COLLATERALTYPEIRB  																								AS COLLATERALTYPEIRB   	--����������ѺƷ����
								,COLLATERALAMOUNT   																								AS COLLATERALAMOUNT     --��Ѻ�ܶ�                              -
								,CURRENCY           																								AS CURRENCY            	--����
								,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')          		AS STARTDATE           	--��ʼ����
								,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')            		AS DUEDATE             	--��������
								,ORIGINALMATURITY                                                   AS ORIGINALMATURITY    	--ԭʼ����
								,RESIDUALM                                                          AS RESIDUALM           	--ʣ������
								,INTEHAIRCUTSFLAG   																			          AS INTEHAIRCUTSFLAG    	--���й����ۿ�ϵ����ʶ
								,INTERNALHC         																							  AS INTERNALHC          	--�ڲ��ۿ�ϵ��
								,FCTYPE             																								AS FCTYPE              	--������ѺƷ����
								,ABSFLAG            																							  AS ABSFLAG             	--�ʲ�֤ȯ����ʶ
								,RATINGDURATIONTYPE 																							  AS RATINGDURATIONTYPE  	--������������
								,FCISSUERATING      																				        AS FCISSUERATING       	--������ѺƷ���еȼ�
								,FCISSUERTYPE                                    		                AS FCISSUERTYPE        	--������ѺƷ���������
								,FCISSUERSTATE                            													AS FCISSUERSTATE       	--������ѺƷ������ע�����
								,FCRESIDUALM                                                        AS FCRESIDUALM         	--������ѺƷʣ������
								,REVAFREQUENCY      																	 				      AS REVAFREQUENCY       	--�ع�Ƶ��
								,GROUPID                                                   					AS GROUPID             	--������
								,RCERating                                                          AS RCERating            --�����˾���ע����ⲿ����
		FROM   			RWA_DEV.RWA_ABS_ISSURE_COLLATERAL
		WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
		;
		COMMIT;
		--�������Ϣ
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_COLLATERAL',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_COLLATERAL',partname => 'COLLATERAL'||p_data_dt_str,granularity => 'PARTITION',cascade => true);


    /*Ŀ�������ͳ��*/
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_COLLATERAL WHERE DATANO = p_data_dt_str;
    --Dbms_output.Put_line('RWA_DEV.rwa_ei_collateral��ǰ��������ݼ�¼Ϊ:' || v_count2 || '��');
		--Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

		p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�'||'-'||v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '���ܵ���ѺƷ(RWA_DEV.pro_rwa_ei_collateral)ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;

END PRO_RWA_EI_COLLATERAL;
/

