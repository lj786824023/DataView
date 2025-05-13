CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_SMZ_COLLATERAL(
			 											P_DATA_DT_STR	IN	VARCHAR2,		--��������
       											P_PO_RTNCODE	OUT	VARCHAR2,		--���ر��
														P_PO_RTNMSG		OUT	VARCHAR2		--��������
)
  /*
    �洢��������:PRO_RWA_SMZ_COLLATERAL
    ʵ�ֹ���:˽ļծ-����Ѻ,��ṹΪ����ѺƷ��
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�
    ��  ��  :V1.0.0
    ��д��  :SHUXD
    ��дʱ��:2014-07-08
    ��  λ	:�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1	:RWA_WS_PRIVATE_BOND|˽ļծҵ��¼ģ��
    Ŀ���	:RWA_SMZ_COLLATERAL|˽ļծ-����ѺƷ��
    ������	:��
    �����¼(�޸���|�޸�ʱ��|�޸�����)��
  */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'PRO_RWA_SMZ_COLLATERAL';
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

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_SMZ_COLLATERAL';

    /*��Ч����º�ͬ��Ӧ�ĵ���ѺƷ��Ϣ*/
    INSERT INTO RWA_DEV.RWA_SMZ_COLLATERAL(
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
		)
		SELECT
								TO_DATE(P_DATA_DT_STR,'YYYYMMDD')																  AS DATADATE          	  --��������
								,P_DATA_DT_STR																									  AS DATANO              	--������ˮ��
								,T1.DBID                                                          AS COLLATERALID        	--����ѺƷID
								,'SMZ'																													  AS SSYSID              	--ԴϵͳID
								,T1.DBID																											    AS SGUARCONTRACTID     	--Դ������ͬID
								,T1.DBID										                                      AS SCOLLATERALID       	--Դ����ѺƷID
								,''																									              AS COLLATERALNAME      	--����ѺƷ����
								,T1.CUSTID2																												AS ISSUERID            	--������ID
								,T1.CUSTID1																											  AS PROVIDERID          	--�ṩ��ID
								,'06'																															AS CREDITRISKDATATYPE  	--���÷�����������
								,T1.DBLX																									        AS GUARANTEEWAY      	  --������ʽ
								,''																			                          AS SOURCECOLTYPE     	  --Դ����ѺƷ����
								,''																									              AS SOURCECOLSUBTYPE    	--Դ����ѺƷС��
								,'0'																															AS SPECPURPBONDFLAG    	--�Ƿ�Ϊ�չ��������в�����������е�ծȯ
								,'0'																															AS QUALFLAGSTD         	--Ȩ�ط��ϸ��ʶ
								,''																															  AS QUALFLAGFIRB        	--�����������ϸ��ʶ
								,''																												        AS COLLATERALTYPESTD   	--Ȩ�ط�����ѺƷ����
								,''																												        AS COLLATERALSDVSSTD   	--Ȩ�ط�����ѺƷϸ��
								,''																																AS COLLATERALTYPEIRB   	--����������ѺƷ����
								,ROUND(TO_NUMBER(T1.DBJZ),6)															        AS COLLATERALAMOUNT     --��Ѻ�ܶ�
								,NVL(T1.DBBZDM,'CNY')																						  AS CURRENCY            	--����
								,REPLACE(T1.DBQSR,'-','')																					AS STARTDATE           	--��ʼ����
								,REPLACE(T1.DBDQR,'-','')																					AS DUEDATE             	--��������
								,CASE WHEN (to_date(T1.DBDQR,'yyyy-mm-dd') - to_date(T1.DBQSR,'yyyy-mm-dd')) / 365<0
                      THEN 0
                      ELSE (to_date(T1.DBDQR,'yyyy-mm-dd') - to_date(T1.DBQSR,'yyyy-mm-dd')) / 365
                END																					                      AS ORIGINALMATURITY				 --ԭʼ����									��λ����
								,CASE WHEN (to_date(T1.DBDQR,'yyyy-mm-dd') - to_date(p_data_dt_str,'yyyymmdd')) / 365<0
								      THEN 0
								      ELSE (to_date(T1.DBDQR,'yyyy-mm-dd') - to_date(p_data_dt_str,'yyyymmdd')) / 365
								END																					                      AS RESIDUALM								 --ʣ������
								,'0'																															AS INTEHAIRCUTSFLAG    	--���й����ۿ�ϵ����ʶ
								,1																																AS INTERNALHC          	--�ڲ��ۿ�ϵ��
								,''																												        AS FCTYPE              	--������ѺƷ����
								,'0'																															AS ABSFLAG             	--�ʲ�֤ȯ����ʶ
								,''																																AS RATINGDURATIONTYPE  	--������������
								,''																																AS FCISSUERATING       	--������ѺƷ���еȼ�
								,''                                                        		    AS FCISSUERTYPE        	--������ѺƷ���������
								,''                                                 							AS FCISSUERSTATE       	--������ѺƷ������ע�����
								,NULL                                                             AS FCRESIDUALM         	--������ѺƷʣ������
								,1																															 	AS REVAFREQUENCY       	--�ع�Ƶ��
								,''                                                               AS GROUPID             	--������
		FROM   RWA.RWA_WS_PRIVATE_BOND T1
		INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT T2                    --���ݲ�¼��
    ON          T1.SUPPORGID=T2.ORGID
    AND         T2.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND         T2.SUPPTMPLID='M-0110'
    AND         T2.SUBMITFLAG='1'
    WHERE  T1.DBLX NOT IN ('030010','030020','030030','020080','020090')
    AND    T1.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
		COMMIT;


    /*Ŀ�������ͳ��*/
    SELECT COUNT(1) INTO v_count FROM RWA_SMZ_COLLATERAL;
    --Dbms_output.Put_line('RWA_SMZ_COLLATERAL��ǰ��������ݼ�¼Ϊ:' || v_count1 || '��');

		--Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

		p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�'||'-'||v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '˽ļծ-����ѺƷ(PRO_RWA_SMZ_COLLATERAL)ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;

END PRO_RWA_SMZ_COLLATERAL;
/

