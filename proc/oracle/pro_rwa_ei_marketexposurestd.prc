CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_MARKETEXPOSURESTD(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_EI_MARKETEXPOSURESTD
    ʵ�ֹ���:���ܱ�׼����¶���������б�׼����¶��Ϣ
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :SHUXD
    ��дʱ��:2016-07-07
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.RWA_LC_MARKETEXPOSURESTD|���ծȯ��׼����¶��
    Դ  ��2 :RWA_DEV.RWA_ZQ_MARKETEXPOSURESTD|ծȯ��׼����¶��
    Դ  ��3 :RWA_DEV.RWA_WH_MARKETEXPOSURESTD|����׼����¶��
    Դ  ��4 :RWA_DEV.RWA_ZJ_MARKETEXPOSURESTD|�ʽ��׼����¶��
    Դ  ��5 :RWA_DEV.RWA_EI_CLIENT|�ͻ����ܱ�
    
    
    Ŀ���1 :RWA_DEV.RWA_EI_MARKETEXPOSURESTD|���ܱ�׼����¶��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_MARKETEXPOSURESTD';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    BEGIN
    --ɾ�������ڵķ���������˴洢�����ǵ�һ�����׳��쳣�����Ժ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_MARKETEXPOSURESTD DROP PARTITION MARKETEXPOSURESTD' || p_data_dt_str;

    COMMIT;

    EXCEPTION
      WHEN OTHERS THEN
        IF (SQLCODE <> '-2149') THEN
          --�״η���truncate�����2149�쳣
         p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '���ܲ��������('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
        END IF;
    END;

    --����һ����ǰ�����µķ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_MARKETEXPOSURESTD ADD PARTITION MARKETEXPOSURESTD' || p_data_dt_str || ' VALUES(TO_DATE('|| p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;

    /*�������ծȯ�ı�׼����¶��Ϣ*/
   /*INSERT INTO RWA_DEV.RWA_EI_MARKETEXPOSURESTD(
                DATADATE                               --��������
                ,DATANO                                --������ˮ��
                ,EXPOSUREID                            --���ձ�¶ID
                ,BOOKTYPE                              --�˻����
                ,INSTRUMENTSID                         --���ڹ���ID
                ,INSTRUMENTSTYPE                       --���ڹ�������
                ,ORGID                                 --��������ID
                ,ORGNAME                               --������������
                ,ORGTYPE                               --������������
                ,MARKETRISKTYPE                        --�г���������
                ,INTERATERISKTYPE                      --���ʷ�������
                ,EQUITYRISKTYPE                        --��Ʊ��������
                ,EXCHANGERISKTYPE                      --����������
                ,COMMODITYNAME                         --��Ʒ��������
                ,OPTIONRISKTYPE                        --��Ȩ��������
                ,ISSUERID                              --������ID
                ,ISSUERNAME                            --����������
                ,ISSUERTYPE                            --�����˴���
                ,ISSUERSUBTYPE                         --������С��
                ,ISSUERREGISTSTATE                     --������ע�����
                ,ISSUERRCERATING                       --�����˾���ע����ⲿ����
                ,SMBFLAG                               --С΢��ҵ��ʶ
                ,UNDERBONDFLAG                         --�Ƿ����ծȯ
                ,PAYMENTDATE                           --�ɿ���
                ,SECURITIESTYPE                        --֤ȯ���
                ,BONDISSUEINTENT     									 --ծȯ����Ŀ��
                ,CLAIMSLEVEL                           --ծȨ����
                ,REABSFLAG                             --���ʲ�֤ȯ����ʶ
                ,ORIGINATORFLAG                        --�Ƿ������
                ,SECURITIESERATING                     --֤ȯ�ⲿ����
                ,STOCKCODE                             --��Ʊ/��ָ����
                ,STOCKMARKET                           --�����г�
                ,EXCHANGEAREA                          --���׵���
                ,STRUCTURALEXPOFLAG                    --�Ƿ�ṹ�Գ���
                ,OPTIONUNDERLYINGFLAG                  --�Ƿ���Ȩ��������
                ,OPTIONUNDERLYINGTYPE                  --��Ȩ������������
                ,OPTIONID                              --��Ȩ����ID
                ,VOLATILITY                            --������
                ,STARTDATE                             --��ʼ����
                ,DUEDATE                               --��������
                ,ORIGINALMATURITY                      --ԭʼ����
                ,RESIDUALM                             --ʣ������
                ,NEXTREPRICEDATE                       --�´��ض�����
                ,NEXTREPRICEM                          --�´��ض�������
                ,RATETYPE                              --��������
                ,COUPONRATE                            --Ʊ������
                ,MODIFIEDDURATION                      --��������
                ,POSITIONTYPE                          --ͷ������
                ,POSITION                              --ͷ��
                ,CURRENCY                              --����
                ,OPTIONUNDERLYINGNAME									 --��Ȩ������������
                ,ORGSORTNO														 --���������

    )
    SELECT
                DATADATE                                     AS DATADATE                 --��������
                ,DATANO												     	         AS DATANO                   --������ˮ��
                ,EXPOSUREID                    				 	     AS EXPOSUREID               --���ձ�¶ID
                ,BOOKTYPE					                           AS BOOKTYPE                 --�˻����                					 Ĭ�ϣ������˻�(02)
                ,INSTRUMENTSID         	                     AS INSTRUMENTSID            --���ڹ���ID
                ,INSTRUMENTSTYPE                    			   AS INSTRUMENTSTYPE          --���ڹ�������
                ,ORGID		 						                       AS ORGID                    --��������ID
                ,ORGNAME										                 AS ORGNAME                  --������������
                ,ORGTYPE						                         AS ORGTYPE                  --������������            					 Ĭ�ϣ����ڻ���(01)
                ,MARKETRISKTYPE			                         AS MARKETRISKTYPE           --�г���������            					 Ĭ�ϣ���
                ,INTERATERISKTYPE								             AS INTERATERISKTYPE         --���ʷ�������            					 Ĭ�ϣ�ծȯ(01)
                ,EQUITYRISKTYPE												       AS EQUITYRISKTYPE           --��Ʊ��������            					 Ĭ�ϣ���
                ,EXCHANGERISKTYPE							               AS EXCHANGERISKTYPE         --����������            					 Ĭ�ϣ���
                ,COMMODITYNAME															 AS COMMODITYNAME       		 --��Ʒ��������            					 Ĭ�ϣ���
                ,OPTIONRISKTYPE                              AS OPTIONRISKTYPE           --��Ȩ��������            					 Ĭ�ϣ���
                ,ISSUERID                            		     AS ISSUERID                 --������ID
                ,ISSUERNAME	                                 AS ISSUERNAME               --����������
                ,ISSUERTYPE	                                 AS ISSUERTYPE               --�����˴���
                ,ISSUERSUBTYPE                               AS ISSUERSUBTYPE            --������С��
                ,ISSUERREGISTSTATE                           AS ISSUERREGISTSTATE        --������ע�����
                ,ISSUERRCERATING	                           AS ISSUERRCERATING          --�����˾���ע����ⲿ����
                ,SMBFLAG	                                   AS SMBFLAG                  --С΢��ҵ��ʶ
                ,UNDERBONDFLAG                               AS UNDERBONDFLAG            --�Ƿ����ծȯ            					 Ĭ�ϣ���(0)
                ,PAYMENTDATE	                               AS PAYMENTDATE              --�ɿ���                  					 Ĭ�ϣ���
                ,SECURITIESTYPE                           	 AS SECURITIESTYPE           --֤ȯ���
                ,BONDISSUEINTENT													   AS BONDISSUEINTENT     		 --ծȯ����Ŀ��
                ,CLAIMSLEVEL	                               AS CLAIMSLEVEL              --ծȨ����                					 ծȯ����2���μ�ծȯ(20)����ծȨ���𣽴μ�ծȨ(02)������Ϊ�߼�ծȨ(01)
                ,REABSFLAG                                   AS REABSFLAG                --���ʲ�֤ȯ����ʶ
                ,ORIGINATORFLAG                              AS ORIGINATORFLAG           --�Ƿ������
                ,SECURITIESERATING                           AS SECURITIESERATING        --֤ȯ�ⲿ����
                ,STOCKCODE                                   AS STOCKCODE                --��Ʊ/��ָ����           					 Ĭ�ϣ���
                ,STOCKMARKET                                 AS STOCKMARKET              --�����г�                					 Ĭ�ϣ���
                ,EXCHANGEAREA                                AS EXCHANGEAREA             --���׵���                					 Ĭ�ϣ���
                ,STRUCTURALEXPOFLAG                          AS STRUCTURALEXPOFLAG       --�Ƿ�ṹ�Գ���          					 Ĭ�ϣ���
                ,OPTIONUNDERLYINGFLAG                        AS OPTIONUNDERLYINGFLAG     --�Ƿ���Ȩ��������        					 Ĭ�ϣ���(0)
                ,OPTIONUNDERLYINGTYPE                        AS OPTIONUNDERLYINGTYPE     --��Ȩ������������        					 Ĭ�ϣ���
                ,OPTIONID                                    AS OPTIONID                 --��Ȩ����ID              					 Ĭ�ϣ���
                ,VOLATILITY                                  AS VOLATILITY               --������                  					 Ĭ�ϣ���
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS STARTDATE                --��ʼ����
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS DUEDATE                  --��������
                ,ORIGINALMATURITY                            AS ORIGINALMATURITY         --ԭʼ����
                ,RESIDUALM                                   AS RESIDUALM                --ʣ������
                ,TO_CHAR(TO_DATE(NEXTREPRICEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                								                             AS NEXTREPRICEDATE          --�´��ض�����
                ,NEXTREPRICEM                                AS NEXTREPRICEM             --�´��ض�������
                ,RATETYPE                                    AS RATETYPE                 --��������
                ,COUPONRATE                                  AS COUPONRATE               --Ʊ������
                ,MODIFIEDDURATION                            AS MODIFIEDDURATION         --��������
                ,POSITIONTYPE                                AS POSITIONTYPE             --ͷ������                					 Ĭ�ϣ���ͷ(01)
                ,POSITION                                    AS POSITION                 --ͷ��
                ,CURRENCY                                    AS CURRENCY                 --����
                ,OPTIONUNDERLYINGNAME									 			 AS OPTIONUNDERLYINGNAME		 --��Ȩ������������
                ,ORGSORTNO														 			 AS ORGSORTNO								 --���������

    FROM				RWA_DEV.RWA_LC_MARKETEXPOSURESTD
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
	  ;

    COMMIT;*/


    /*����ծȯ�ı�׼����¶��Ϣ*/
   /* INSERT INTO RWA_DEV.RWA_EI_MARKETEXPOSURESTD(
                DATADATE                               --��������
                ,DATANO                                --������ˮ��
                ,EXPOSUREID                            --���ձ�¶ID
                ,BOOKTYPE                              --�˻����
                ,INSTRUMENTSID                         --���ڹ���ID
                ,INSTRUMENTSTYPE                       --���ڹ�������
                ,ORGID                                 --��������ID
                ,ORGNAME                               --������������
                ,ORGTYPE                               --������������
                ,MARKETRISKTYPE                        --�г���������
                ,INTERATERISKTYPE                      --���ʷ�������
                ,EQUITYRISKTYPE                        --��Ʊ��������
                ,EXCHANGERISKTYPE                      --����������
                ,COMMODITYNAME                         --��Ʒ��������
                ,OPTIONRISKTYPE                        --��Ȩ��������
                ,ISSUERID                              --������ID
                ,ISSUERNAME                            --����������
                ,ISSUERTYPE                            --�����˴���
                ,ISSUERSUBTYPE                         --������С��
                ,ISSUERREGISTSTATE                     --������ע�����
                ,ISSUERRCERATING                       --�����˾���ע����ⲿ����
                ,SMBFLAG                               --С΢��ҵ��ʶ
                ,UNDERBONDFLAG                         --�Ƿ����ծȯ
                ,PAYMENTDATE                           --�ɿ���
                ,SECURITIESTYPE                        --֤ȯ���
                ,BONDISSUEINTENT     									 --ծȯ����Ŀ��
                ,CLAIMSLEVEL                           --ծȨ����
                ,REABSFLAG                             --���ʲ�֤ȯ����ʶ
                ,ORIGINATORFLAG                        --�Ƿ������
                ,SECURITIESERATING                     --֤ȯ�ⲿ����
                ,STOCKCODE                             --��Ʊ/��ָ����
                ,STOCKMARKET                           --�����г�
                ,EXCHANGEAREA                          --���׵���
                ,STRUCTURALEXPOFLAG                    --�Ƿ�ṹ�Գ���
                ,OPTIONUNDERLYINGFLAG                  --�Ƿ���Ȩ��������
                ,OPTIONUNDERLYINGTYPE                  --��Ȩ������������
                ,OPTIONID                              --��Ȩ����ID
                ,VOLATILITY                            --������
                ,STARTDATE                             --��ʼ����
                ,DUEDATE                               --��������
                ,ORIGINALMATURITY                      --ԭʼ����
                ,RESIDUALM                             --ʣ������
                ,NEXTREPRICEDATE                       --�´��ض�����
                ,NEXTREPRICEM                          --�´��ض�������
                ,RATETYPE                              --��������
                ,COUPONRATE                            --Ʊ������
                ,MODIFIEDDURATION                      --��������
                ,POSITIONTYPE                          --ͷ������
                ,POSITION                              --ͷ��
                ,CURRENCY                              --����
                ,OPTIONUNDERLYINGNAME									 --��Ȩ������������
                ,ORGSORTNO														 --���������

    )
    SELECT
                DATADATE                                     AS DATADATE                 --��������
                ,DATANO												     	         AS DATANO                   --������ˮ��
                ,EXPOSUREID                    				 	     AS EXPOSUREID               --���ձ�¶ID
                ,BOOKTYPE					                           AS BOOKTYPE                 --�˻����                					 Ĭ�ϣ������˻�(02)
                ,INSTRUMENTSID         	                     AS INSTRUMENTSID            --���ڹ���ID
                ,INSTRUMENTSTYPE                    			   AS INSTRUMENTSTYPE          --���ڹ�������
                ,ORGID		 						                       AS ORGID                    --��������ID
                ,ORGNAME										                 AS ORGNAME                  --������������
                ,ORGTYPE						                         AS ORGTYPE                  --������������            					 Ĭ�ϣ����ڻ���(01)
                ,MARKETRISKTYPE			                         AS MARKETRISKTYPE           --�г���������            					 Ĭ�ϣ���
                ,INTERATERISKTYPE								             AS INTERATERISKTYPE         --���ʷ�������            					 Ĭ�ϣ�ծȯ(01)
                ,EQUITYRISKTYPE												       AS EQUITYRISKTYPE           --��Ʊ��������            					 Ĭ�ϣ���
                ,EXCHANGERISKTYPE							               AS EXCHANGERISKTYPE         --����������            					 Ĭ�ϣ���
                ,COMMODITYNAME															 AS COMMODITYNAME       		 --��Ʒ��������            					 Ĭ�ϣ���
                ,OPTIONRISKTYPE                              AS OPTIONRISKTYPE           --��Ȩ��������            					 Ĭ�ϣ���
                ,ISSUERID                            		     AS ISSUERID                 --������ID
                ,ISSUERNAME	                                 AS ISSUERNAME               --����������
                ,ISSUERTYPE	                                 AS ISSUERTYPE               --�����˴���
                ,ISSUERSUBTYPE                               AS ISSUERSUBTYPE            --������С��
                ,ISSUERREGISTSTATE                           AS ISSUERREGISTSTATE        --������ע�����
                ,ISSUERRCERATING	                           AS ISSUERRCERATING          --�����˾���ע����ⲿ����
                ,SMBFLAG	                                   AS SMBFLAG                  --С΢��ҵ��ʶ
                ,UNDERBONDFLAG                               AS UNDERBONDFLAG            --�Ƿ����ծȯ            					 Ĭ�ϣ���(0)
                ,PAYMENTDATE	                               AS PAYMENTDATE              --�ɿ���                  					 Ĭ�ϣ���
                ,SECURITIESTYPE                           	 AS SECURITIESTYPE           --֤ȯ���
                ,BONDISSUEINTENT													   AS BONDISSUEINTENT     		 --ծȯ����Ŀ��
                ,CLAIMSLEVEL	                               AS CLAIMSLEVEL              --ծȨ����                					 ծȯ����2���μ�ծȯ(20)����ծȨ���𣽴μ�ծȨ(02)������Ϊ�߼�ծȨ(01)
                ,REABSFLAG                                   AS REABSFLAG                --���ʲ�֤ȯ����ʶ
                ,ORIGINATORFLAG                              AS ORIGINATORFLAG           --�Ƿ������
                ,SECURITIESERATING                           AS SECURITIESERATING        --֤ȯ�ⲿ����
                ,STOCKCODE                                   AS STOCKCODE                --��Ʊ/��ָ����           					 Ĭ�ϣ���
                ,STOCKMARKET                                 AS STOCKMARKET              --�����г�                					 Ĭ�ϣ���
                ,EXCHANGEAREA                                AS EXCHANGEAREA             --���׵���                					 Ĭ�ϣ���
                ,STRUCTURALEXPOFLAG                          AS STRUCTURALEXPOFLAG       --�Ƿ�ṹ�Գ���          					 Ĭ�ϣ���
                ,OPTIONUNDERLYINGFLAG                        AS OPTIONUNDERLYINGFLAG     --�Ƿ���Ȩ��������        					 Ĭ�ϣ���(0)
                ,OPTIONUNDERLYINGTYPE                        AS OPTIONUNDERLYINGTYPE     --��Ȩ������������        					 Ĭ�ϣ���
                ,OPTIONID                                    AS OPTIONID                 --��Ȩ����ID              					 Ĭ�ϣ���
                ,VOLATILITY                                  AS VOLATILITY               --������                  					 Ĭ�ϣ���
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS STARTDATE                --��ʼ����
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS DUEDATE                  --��������
                ,ORIGINALMATURITY                            AS ORIGINALMATURITY         --ԭʼ����
                ,RESIDUALM                                   AS RESIDUALM                --ʣ������
                ,TO_CHAR(TO_DATE(NEXTREPRICEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                								                             AS NEXTREPRICEDATE          --�´��ض�����
                ,NEXTREPRICEM                                AS NEXTREPRICEM             --�´��ض�������
                ,RATETYPE                                    AS RATETYPE                 --��������
                ,COUPONRATE                                  AS COUPONRATE               --Ʊ������
                ,MODIFIEDDURATION                            AS MODIFIEDDURATION         --��������
                ,POSITIONTYPE                                AS POSITIONTYPE             --ͷ������                					 Ĭ�ϣ���ͷ(01)
                ,POSITION                                    AS POSITION                 --ͷ��
                ,CURRENCY                                    AS CURRENCY                 --����
                ,OPTIONUNDERLYINGNAME									 			 AS OPTIONUNDERLYINGNAME		 --��Ȩ������������
                ,ORGSORTNO														 			 AS ORGSORTNO								 --���������

    FROM				RWA_DEV.RWA_ZQ_MARKETEXPOSURESTD
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
	  ;

    COMMIT;*/


    /*�������ı�׼����¶��Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_MARKETEXPOSURESTD(
                DATADATE                               --��������
                ,DATANO                                --������ˮ��
                ,EXPOSUREID                            --���ձ�¶ID
                ,BOOKTYPE                              --�˻����
                ,INSTRUMENTSID                         --���ڹ���ID
                ,INSTRUMENTSTYPE                       --���ڹ�������
                ,ORGID                                 --��������ID
                ,ORGNAME                               --������������
                ,ORGTYPE                               --������������
                ,MARKETRISKTYPE                        --�г���������
                ,INTERATERISKTYPE                      --���ʷ�������
                ,EQUITYRISKTYPE                        --��Ʊ��������
                ,EXCHANGERISKTYPE                      --����������
                ,COMMODITYNAME                         --��Ʒ��������
                ,OPTIONRISKTYPE                        --��Ȩ��������
                ,ISSUERID                              --������ID
                ,ISSUERNAME                            --����������
                ,ISSUERTYPE                            --�����˴���
                ,ISSUERSUBTYPE                         --������С��
                ,ISSUERREGISTSTATE                     --������ע�����
                ,ISSUERRCERATING                       --�����˾���ע����ⲿ����
                ,SMBFLAG                               --С΢��ҵ��ʶ
                ,UNDERBONDFLAG                         --�Ƿ����ծȯ
                ,PAYMENTDATE                           --�ɿ���
                ,SECURITIESTYPE                        --֤ȯ���
                ,BONDISSUEINTENT     									 --ծȯ����Ŀ��
                ,CLAIMSLEVEL                           --ծȨ����
                ,REABSFLAG                             --���ʲ�֤ȯ����ʶ
                ,ORIGINATORFLAG                        --�Ƿ������
                ,SECURITIESERATING                     --֤ȯ�ⲿ����
                ,STOCKCODE                             --��Ʊ/��ָ����
                ,STOCKMARKET                           --�����г�
                ,EXCHANGEAREA                          --���׵���
                ,STRUCTURALEXPOFLAG                    --�Ƿ�ṹ�Գ���
                ,OPTIONUNDERLYINGFLAG                  --�Ƿ���Ȩ��������
                ,OPTIONUNDERLYINGTYPE                  --��Ȩ������������
                ,OPTIONID                              --��Ȩ����ID
                ,VOLATILITY                            --������
                ,STARTDATE                             --��ʼ����
                ,DUEDATE                               --��������
                ,ORIGINALMATURITY                      --ԭʼ����
                ,RESIDUALM                             --ʣ������
                ,NEXTREPRICEDATE                       --�´��ض�����
                ,NEXTREPRICEM                          --�´��ض�������
                ,RATETYPE                              --��������
                ,COUPONRATE                            --Ʊ������
                ,MODIFIEDDURATION                      --��������
                ,POSITIONTYPE                          --ͷ������
                ,POSITION                              --ͷ��
                ,CURRENCY                              --����
                ,OPTIONUNDERLYINGNAME									 --��Ȩ������������
                ,ORGSORTNO														 --���������

    )
    SELECT
                DATADATE                                     AS DATADATE                 --��������
                ,DATANO												     	         AS DATANO                   --������ˮ��
                ,EXPOSUREID                    				 	     AS EXPOSUREID               --���ձ�¶ID
                ,BOOKTYPE					                           AS BOOKTYPE                 --�˻����                					 Ĭ�ϣ������˻�(02)
                ,INSTRUMENTSID         	                     AS INSTRUMENTSID            --���ڹ���ID
                ,INSTRUMENTSTYPE                    			   AS INSTRUMENTSTYPE          --���ڹ�������
                ,ORGID		 						                       AS ORGID                    --��������ID
                ,ORGNAME										                 AS ORGNAME                  --������������
                ,ORGTYPE						                         AS ORGTYPE                  --������������            					 Ĭ�ϣ����ڻ���(01)
                ,MARKETRISKTYPE			                         AS MARKETRISKTYPE           --�г���������            					 Ĭ�ϣ���
                ,INTERATERISKTYPE								             AS INTERATERISKTYPE         --���ʷ�������            					 Ĭ�ϣ�ծȯ(01)
                ,EQUITYRISKTYPE												       AS EQUITYRISKTYPE           --��Ʊ��������            					 Ĭ�ϣ���
                ,EXCHANGERISKTYPE							               AS EXCHANGERISKTYPE         --����������            					 Ĭ�ϣ���
                ,COMMODITYNAME															 AS COMMODITYNAME       		 --��Ʒ��������            					 Ĭ�ϣ���
                ,OPTIONRISKTYPE                              AS OPTIONRISKTYPE           --��Ȩ��������            					 Ĭ�ϣ���
                ,ISSUERID                            		     AS ISSUERID                 --������ID
                ,ISSUERNAME	                                 AS ISSUERNAME               --����������
                ,ISSUERTYPE	                                 AS ISSUERTYPE               --�����˴���
                ,ISSUERSUBTYPE                               AS ISSUERSUBTYPE            --������С��
                ,ISSUERREGISTSTATE                           AS ISSUERREGISTSTATE        --������ע�����
                ,ISSUERRCERATING	                           AS ISSUERRCERATING          --�����˾���ע����ⲿ����
                ,SMBFLAG	                                   AS SMBFLAG                  --С΢��ҵ��ʶ
                ,UNDERBONDFLAG                               AS UNDERBONDFLAG            --�Ƿ����ծȯ            					 Ĭ�ϣ���(0)
                ,PAYMENTDATE	                               AS PAYMENTDATE              --�ɿ���                  					 Ĭ�ϣ���
                ,SECURITIESTYPE                           	 AS SECURITIESTYPE           --֤ȯ���
                ,BONDISSUEINTENT													   AS BONDISSUEINTENT     		 --ծȯ����Ŀ��
                ,CLAIMSLEVEL	                               AS CLAIMSLEVEL              --ծȨ����                					 ծȯ����2���μ�ծȯ(20)����ծȨ���𣽴μ�ծȨ(02)������Ϊ�߼�ծȨ(01)
                ,REABSFLAG                                   AS REABSFLAG                --���ʲ�֤ȯ����ʶ
                ,ORIGINATORFLAG                              AS ORIGINATORFLAG           --�Ƿ������
                ,SECURITIESERATING                           AS SECURITIESERATING        --֤ȯ�ⲿ����
                ,STOCKCODE                                   AS STOCKCODE                --��Ʊ/��ָ����           					 Ĭ�ϣ���
                ,STOCKMARKET                                 AS STOCKMARKET              --�����г�                					 Ĭ�ϣ���
                ,EXCHANGEAREA                                AS EXCHANGEAREA             --���׵���                					 Ĭ�ϣ���
                ,STRUCTURALEXPOFLAG                          AS STRUCTURALEXPOFLAG       --�Ƿ�ṹ�Գ���          					 Ĭ�ϣ���
                ,OPTIONUNDERLYINGFLAG                        AS OPTIONUNDERLYINGFLAG     --�Ƿ���Ȩ��������        					 Ĭ�ϣ���(0)
                ,OPTIONUNDERLYINGTYPE                        AS OPTIONUNDERLYINGTYPE     --��Ȩ������������        					 Ĭ�ϣ���
                ,OPTIONID                                    AS OPTIONID                 --��Ȩ����ID              					 Ĭ�ϣ���
                ,VOLATILITY                                  AS VOLATILITY               --������                  					 Ĭ�ϣ���
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS STARTDATE                --��ʼ����
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS DUEDATE                  --��������
                ,ORIGINALMATURITY                            AS ORIGINALMATURITY         --ԭʼ����
                ,RESIDUALM                                   AS RESIDUALM                --ʣ������
                ,TO_CHAR(TO_DATE(NEXTREPRICEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                								                             AS NEXTREPRICEDATE          --�´��ض�����
                ,NEXTREPRICEM                                AS NEXTREPRICEM             --�´��ض�������
                ,RATETYPE                                    AS RATETYPE                 --��������
                ,COUPONRATE                                  AS COUPONRATE               --Ʊ������
                ,MODIFIEDDURATION                            AS MODIFIEDDURATION         --��������
                ,POSITIONTYPE                                AS POSITIONTYPE             --ͷ������                					 Ĭ�ϣ���ͷ(01)
                ,POSITION                                    AS POSITION                 --ͷ��
                ,CURRENCY                                    AS CURRENCY                 --����
                ,OPTIONUNDERLYINGNAME									 			 AS OPTIONUNDERLYINGNAME		 --��Ȩ������������
                ,ORGSORTNO														 			 AS ORGSORTNO								 --���������

    FROM				RWA_DEV.RWA_WH_MARKETEXPOSURESTD
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
	  ;

		COMMIT;
    
    
    /*�������ı�׼����¶��Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_MARKETEXPOSURESTD(
                DATADATE                               --��������
                ,DATANO                                --������ˮ��
                ,EXPOSUREID                            --���ձ�¶ID
                ,BOOKTYPE                              --�˻����
                ,INSTRUMENTSID                         --���ڹ���ID
                ,INSTRUMENTSTYPE                       --���ڹ�������
                ,ORGID                                 --��������ID
                ,ORGNAME                               --������������
                ,ORGTYPE                               --������������
                ,MARKETRISKTYPE                        --�г���������
                ,INTERATERISKTYPE                      --���ʷ�������
                ,EQUITYRISKTYPE                        --��Ʊ��������
                ,EXCHANGERISKTYPE                      --����������
                ,COMMODITYNAME                         --��Ʒ��������
                ,OPTIONRISKTYPE                        --��Ȩ��������
                ,ISSUERID                              --������ID
                ,ISSUERNAME                            --����������
                ,ISSUERTYPE                            --�����˴���
                ,ISSUERSUBTYPE                         --������С��
                ,ISSUERREGISTSTATE                     --������ע�����
                ,ISSUERRCERATING                       --�����˾���ע����ⲿ����
                ,SMBFLAG                               --С΢��ҵ��ʶ
                ,UNDERBONDFLAG                         --�Ƿ����ծȯ
                ,PAYMENTDATE                           --�ɿ���
                ,SECURITIESTYPE                        --֤ȯ���
                ,BONDISSUEINTENT                       --ծȯ����Ŀ��
                ,CLAIMSLEVEL                           --ծȨ����
                ,REABSFLAG                             --���ʲ�֤ȯ����ʶ
                ,ORIGINATORFLAG                        --�Ƿ������
                ,SECURITIESERATING                     --֤ȯ�ⲿ����
                ,STOCKCODE                             --��Ʊ/��ָ����
                ,STOCKMARKET                           --�����г�
                ,EXCHANGEAREA                          --���׵���
                ,STRUCTURALEXPOFLAG                    --�Ƿ�ṹ�Գ���
                ,OPTIONUNDERLYINGFLAG                  --�Ƿ���Ȩ��������
                ,OPTIONUNDERLYINGTYPE                  --��Ȩ������������
                ,OPTIONID                              --��Ȩ����ID
                ,VOLATILITY                            --������
                ,STARTDATE                             --��ʼ����
                ,DUEDATE                               --��������
                ,ORIGINALMATURITY                      --ԭʼ����
                ,RESIDUALM                             --ʣ������
                ,NEXTREPRICEDATE                       --�´��ض�����
                ,NEXTREPRICEM                          --�´��ض�������
                ,RATETYPE                              --��������
                ,COUPONRATE                            --Ʊ������
                ,MODIFIEDDURATION                      --��������
                ,POSITIONTYPE                          --ͷ������
                ,POSITION                              --ͷ��
                ,CURRENCY                              --����
                ,OPTIONUNDERLYINGNAME                  --��Ȩ������������
                ,ORGSORTNO                             --���������

    )
    SELECT
                T1.DATADATE                                     AS DATADATE                 --��������
                ,T1.DATANO                                      AS DATANO                   --������ˮ��
                ,T1.EXPOSUREID                                  AS EXPOSUREID               --���ձ�¶ID
                ,T1.BOOKTYPE                                    AS BOOKTYPE                 --�˻����                          Ĭ�ϣ������˻�(02)
                ,T1.INSTRUMENTSID                               AS INSTRUMENTSID            --���ڹ���ID
                ,T1.INSTRUMENTSTYPE                             AS INSTRUMENTSTYPE          --���ڹ�������
                ,T1.ORGID                                       AS ORGID                    --��������ID
                ,T1.ORGNAME                                     AS ORGNAME                  --������������
                ,T1.ORGTYPE                                     AS ORGTYPE                  --������������                      Ĭ�ϣ����ڻ���(01)
                ,T1.MARKETRISKTYPE                              AS MARKETRISKTYPE           --�г���������                      Ĭ�ϣ���
                ,T1.INTERATERISKTYPE                            AS INTERATERISKTYPE         --���ʷ�������                      Ĭ�ϣ�ծȯ(01)
                ,T1.EQUITYRISKTYPE                              AS EQUITYRISKTYPE           --��Ʊ��������                      Ĭ�ϣ���
                ,T1.EXCHANGERISKTYPE                            AS EXCHANGERISKTYPE         --����������                      Ĭ�ϣ���
                ,T1.COMMODITYNAME                               AS COMMODITYNAME            --��Ʒ��������                      Ĭ�ϣ���
                ,T1.OPTIONRISKTYPE                              AS OPTIONRISKTYPE           --��Ȩ��������                      Ĭ�ϣ���
                ,T1.ISSUERID                                    AS ISSUERID                 --������ID
                ,T1.ISSUERNAME                                  AS ISSUERNAME               --����������
                ,T1.ISSUERTYPE                                  AS ISSUERTYPE               --�����˴���
                ,T1.ISSUERSUBTYPE                               AS ISSUERSUBTYPE            --������С��
                ,T1.ISSUERREGISTSTATE                           AS ISSUERREGISTSTATE        --������ע�����
                ,T1.ISSUERRCERATING                             AS ISSUERRCERATING          --�����˾���ע����ⲿ����
                ,T1.SMBFLAG                                     AS SMBFLAG                  --С΢��ҵ��ʶ
                ,T1.UNDERBONDFLAG                               AS UNDERBONDFLAG            --�Ƿ����ծȯ                      Ĭ�ϣ���(0)
                ,T1.PAYMENTDATE                                 AS PAYMENTDATE              --�ɿ���                             Ĭ�ϣ���
                ,T1.SECURITIESTYPE                              AS SECURITIESTYPE           --֤ȯ���
                ,T1.BONDISSUEINTENT                             AS BONDISSUEINTENT          --ծȯ����Ŀ��
                ,T1.CLAIMSLEVEL                                 AS CLAIMSLEVEL              --ծȨ����                          ծȯ����2���μ�ծȯ(20)����ծȨ���𣽴μ�ծȨ(02)������Ϊ�߼�ծȨ(01)
                ,T1.REABSFLAG                                   AS REABSFLAG                --���ʲ�֤ȯ����ʶ
                ,T1.ORIGINATORFLAG                              AS ORIGINATORFLAG           --�Ƿ������
                ,T1.SECURITIESERATING                           AS SECURITIESERATING        --֤ȯ�ⲿ����
                ,T1.STOCKCODE                                   AS STOCKCODE                --��Ʊ/��ָ����                     Ĭ�ϣ���
                ,T1.STOCKMARKET                                 AS STOCKMARKET              --�����г�                          Ĭ�ϣ���
                ,T1.EXCHANGEAREA                                AS EXCHANGEAREA             --���׵���                          Ĭ�ϣ���
                ,T1.STRUCTURALEXPOFLAG                          AS STRUCTURALEXPOFLAG       --�Ƿ�ṹ�Գ���                     Ĭ�ϣ���
                ,T1.OPTIONUNDERLYINGFLAG                        AS OPTIONUNDERLYINGFLAG     --�Ƿ���Ȩ��������                  Ĭ�ϣ���(0)
                ,T1.OPTIONUNDERLYINGTYPE                        AS OPTIONUNDERLYINGTYPE     --��Ȩ������������                  Ĭ�ϣ���
                ,T1.OPTIONID                                    AS OPTIONID                 --��Ȩ����ID                        Ĭ�ϣ���
                ,T1.VOLATILITY                                  AS VOLATILITY               --������                             Ĭ�ϣ���
                ,TO_CHAR(TO_DATE(T1.STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS STARTDATE                --��ʼ����
                ,TO_CHAR(TO_DATE(T1.DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS DUEDATE                  --��������
                ,T1.ORIGINALMATURITY                            AS ORIGINALMATURITY         --ԭʼ����
                ,T1.RESIDUALM                                   AS RESIDUALM                --ʣ������
                ,TO_CHAR(TO_DATE(T1.NEXTREPRICEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS NEXTREPRICEDATE          --�´��ض�����
                ,T1.NEXTREPRICEM                                AS NEXTREPRICEM             --�´��ض�������
                ,T1.RATETYPE                                    AS RATETYPE                 --��������
                ,T1.COUPONRATE                                  AS COUPONRATE               --Ʊ������
                ,T1.MODIFIEDDURATION                            AS MODIFIEDDURATION         --��������
                ,T1.POSITIONTYPE                                AS POSITIONTYPE             --ͷ������                          Ĭ�ϣ���ͷ(01)
                ,T1.POSITION                                    AS POSITION                 --ͷ��
                ,T1.CURRENCY                                    AS CURRENCY                 --����
                ,T1.OPTIONUNDERLYINGNAME                        AS OPTIONUNDERLYINGNAME     --��Ȩ������������
                ,T1.ORGSORTNO                                   AS ORGSORTNO                --���������

    FROM        RWA_DEV.RWA_ZJ_MARKETEXPOSURESTD T1   --�ʽ��׼����¶��
    WHERE       T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;

    COMMIT;
    
    /*�������ı�׼����¶��Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_MARKETEXPOSURESTD(
                DATADATE                               --��������
                ,DATANO                                --������ˮ��
                ,EXPOSUREID                            --���ձ�¶ID
                ,BOOKTYPE                              --�˻����
                ,INSTRUMENTSID                         --���ڹ���ID
                ,INSTRUMENTSTYPE                       --���ڹ�������
                ,ORGID                                 --��������ID
                ,ORGNAME                               --������������
                ,ORGTYPE                               --������������
                ,MARKETRISKTYPE                        --�г���������
                ,INTERATERISKTYPE                      --���ʷ�������
                ,EQUITYRISKTYPE                        --��Ʊ��������
                ,EXCHANGERISKTYPE                      --����������
                ,COMMODITYNAME                         --��Ʒ��������
                ,OPTIONRISKTYPE                        --��Ȩ��������
                ,ISSUERID                              --������ID
                ,ISSUERNAME                            --����������
                ,ISSUERTYPE                            --�����˴���
                ,ISSUERSUBTYPE                         --������С��
                ,ISSUERREGISTSTATE                     --������ע�����
                ,ISSUERRCERATING                       --�����˾���ע����ⲿ����
                ,SMBFLAG                               --С΢��ҵ��ʶ
                ,UNDERBONDFLAG                         --�Ƿ����ծȯ
                ,PAYMENTDATE                           --�ɿ���
                ,SECURITIESTYPE                        --֤ȯ���
                ,BONDISSUEINTENT                       --ծȯ����Ŀ��
                ,CLAIMSLEVEL                           --ծȨ����
                ,REABSFLAG                             --���ʲ�֤ȯ����ʶ
                ,ORIGINATORFLAG                        --�Ƿ������
                ,SECURITIESERATING                     --֤ȯ�ⲿ����
                ,STOCKCODE                             --��Ʊ/��ָ����
                ,STOCKMARKET                           --�����г�
                ,EXCHANGEAREA                          --���׵���
                ,STRUCTURALEXPOFLAG                    --�Ƿ�ṹ�Գ���
                ,OPTIONUNDERLYINGFLAG                  --�Ƿ���Ȩ��������
                ,OPTIONUNDERLYINGTYPE                  --��Ȩ������������
                ,OPTIONID                              --��Ȩ����ID
                ,VOLATILITY                            --������
                ,STARTDATE                             --��ʼ����
                ,DUEDATE                               --��������
                ,ORIGINALMATURITY                      --ԭʼ����
                ,RESIDUALM                             --ʣ������
                ,NEXTREPRICEDATE                       --�´��ض�����
                ,NEXTREPRICEM                          --�´��ض�������
                ,RATETYPE                              --��������
                ,COUPONRATE                            --Ʊ������
                ,MODIFIEDDURATION                      --��������
                ,POSITIONTYPE                          --ͷ������
                ,POSITION                              --ͷ��
                ,CURRENCY                              --����
                ,OPTIONUNDERLYINGNAME                  --��Ȩ������������
                ,ORGSORTNO                             --���������

    )
    SELECT
                T1.DATADATE                                     AS DATADATE                 --��������
                ,T1.DATANO                                      AS DATANO                   --������ˮ��
                ,T1.EXPOSUREID                                  AS EXPOSUREID               --���ձ�¶ID
                ,T1.BOOKTYPE                                    AS BOOKTYPE                 --�˻����                          Ĭ�ϣ������˻�(02)
                ,T1.INSTRUMENTSID                               AS INSTRUMENTSID            --���ڹ���ID
                ,T1.INSTRUMENTSTYPE                             AS INSTRUMENTSTYPE          --���ڹ�������
                ,T1.ORGID                                       AS ORGID                    --��������ID
                ,T1.ORGNAME                                     AS ORGNAME                  --������������
                ,T1.ORGTYPE                                     AS ORGTYPE                  --������������                      Ĭ�ϣ����ڻ���(01)
                ,T1.MARKETRISKTYPE                              AS MARKETRISKTYPE           --�г���������                      Ĭ�ϣ���
                ,T1.INTERATERISKTYPE                            AS INTERATERISKTYPE         --���ʷ�������                      Ĭ�ϣ�ծȯ(01)
                ,T1.EQUITYRISKTYPE                              AS EQUITYRISKTYPE           --��Ʊ��������                      Ĭ�ϣ���
                ,T1.EXCHANGERISKTYPE                            AS EXCHANGERISKTYPE         --����������                      Ĭ�ϣ���
                ,T1.COMMODITYNAME                               AS COMMODITYNAME            --��Ʒ��������                      Ĭ�ϣ���
                ,T1.OPTIONRISKTYPE                              AS OPTIONRISKTYPE           --��Ȩ��������                      Ĭ�ϣ���
                ,T1.ISSUERID                                    AS ISSUERID                 --������ID
                ,T1.ISSUERNAME                                  AS ISSUERNAME               --����������
                ,T1.ISSUERTYPE                                  AS ISSUERTYPE               --�����˴���
                ,T1.ISSUERSUBTYPE                               AS ISSUERSUBTYPE            --������С��
                ,T1.ISSUERREGISTSTATE                           AS ISSUERREGISTSTATE        --������ע�����
                ,T1.ISSUERRCERATING                             AS ISSUERRCERATING          --�����˾���ע����ⲿ����
                ,T1.SMBFLAG                                     AS SMBFLAG                  --С΢��ҵ��ʶ
                ,T1.UNDERBONDFLAG                               AS UNDERBONDFLAG            --�Ƿ����ծȯ                      Ĭ�ϣ���(0)
                ,T1.PAYMENTDATE                                 AS PAYMENTDATE              --�ɿ���                             Ĭ�ϣ���
                ,T1.SECURITIESTYPE                              AS SECURITIESTYPE           --֤ȯ���
                ,T1.BONDISSUEINTENT                             AS BONDISSUEINTENT          --ծȯ����Ŀ��
                ,T1.CLAIMSLEVEL                                 AS CLAIMSLEVEL              --ծȨ����                          ծȯ����2���μ�ծȯ(20)����ծȨ���𣽴μ�ծȨ(02)������Ϊ�߼�ծȨ(01)
                ,T1.REABSFLAG                                   AS REABSFLAG                --���ʲ�֤ȯ����ʶ
                ,T1.ORIGINATORFLAG                              AS ORIGINATORFLAG           --�Ƿ������
                ,T1.SECURITIESERATING                           AS SECURITIESERATING        --֤ȯ�ⲿ����
                ,T1.STOCKCODE                                   AS STOCKCODE                --��Ʊ/��ָ����                     Ĭ�ϣ���
                ,T1.STOCKMARKET                                 AS STOCKMARKET              --�����г�                          Ĭ�ϣ���
                ,T1.EXCHANGEAREA                                AS EXCHANGEAREA             --���׵���                          Ĭ�ϣ���
                ,T1.STRUCTURALEXPOFLAG                          AS STRUCTURALEXPOFLAG       --�Ƿ�ṹ�Գ���                     Ĭ�ϣ���
                ,T1.OPTIONUNDERLYINGFLAG                        AS OPTIONUNDERLYINGFLAG     --�Ƿ���Ȩ��������                  Ĭ�ϣ���(0)
                ,T1.OPTIONUNDERLYINGTYPE                        AS OPTIONUNDERLYINGTYPE     --��Ȩ������������                  Ĭ�ϣ���
                ,T1.OPTIONID                                    AS OPTIONID                 --��Ȩ����ID                        Ĭ�ϣ���
                ,T1.VOLATILITY                                  AS VOLATILITY               --������                             Ĭ�ϣ���
                ,TO_CHAR(TO_DATE(T1.STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS STARTDATE                --��ʼ����
                ,TO_CHAR(TO_DATE(T1.DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS DUEDATE                  --��������
                ,T1.ORIGINALMATURITY                            AS ORIGINALMATURITY         --ԭʼ����
                ,T1.RESIDUALM                                   AS RESIDUALM                --ʣ������
                ,TO_CHAR(TO_DATE(T1.NEXTREPRICEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS NEXTREPRICEDATE          --�´��ض�����
                ,T1.NEXTREPRICEM                                AS NEXTREPRICEM             --�´��ض�������
                ,T1.RATETYPE                                    AS RATETYPE                 --��������
                ,T1.COUPONRATE                                  AS COUPONRATE               --Ʊ������
                ,T1.MODIFIEDDURATION                            AS MODIFIEDDURATION         --��������
                ,T1.POSITIONTYPE                                AS POSITIONTYPE             --ͷ������                          Ĭ�ϣ���ͷ(01)
                ,T1.POSITION                                    AS POSITION                 --ͷ��
                ,T1.CURRENCY                                    AS CURRENCY                 --����
                ,T1.OPTIONUNDERLYINGNAME                        AS OPTIONUNDERLYINGNAME     --��Ȩ������������
                ,T1.ORGSORTNO                                   AS ORGSORTNO                --���������

    FROM        RWA_DEV.RWA_YSP_MARKETEXPOSURESTD T1   --�ʽ��׼����¶��
    WHERE       T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;

    COMMIT;
    

		----------------------------------------------�����г����ձ�¶�����˴�С��---------------------------------------------------------
    UPDATE RWA_DEV.RWA_EI_MARKETEXPOSURESTD T1
      SET T1.ISSUERTYPE = (
                           SELECT T2.CLIENTTYPE
                           FROM RWA_DEV.RWA_EI_CLIENT T2
                           WHERE T2.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
                           AND T1.ISSUERID=T2.CLIENTID
                          )
          ,T1.ISSUERSUBTYPE = (
                               SELECT T2.CLIENTSUBTYPE
                               FROM RWA_DEV.RWA_EI_CLIENT T2
                               WHERE T2.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
                               AND T1.ISSUERID=T2.CLIENTID
                              )
    WHERE   T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND   T1.ISSUERTYPE    IS NULL
      AND   T1.ISSUERSUBTYPE IS NULL
    ;
    COMMIT;

    --�������Ϣ
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_MARKETEXPOSURESTD',partname => 'MARKETEXPOSURESTD'||p_data_dt_str,granularity => 'PARTITION',cascade => true);


    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_MARKETEXPOSURESTD WHERE DATANO = p_data_dt_str;
    
    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�'||'-'||v_count;
		--�����쳣
EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '�г����ձ�׼����¶��Ϣ('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_EI_MARKETEXPOSURESTD;
/

