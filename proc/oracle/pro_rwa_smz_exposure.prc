CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_SMZ_EXPOSURE(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:PRO_RWA_SMZ_EXPOSURE
    ʵ�ֹ���:˽ļծ-���÷��ձ�¶
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :SHUXD
    ��дʱ��:2016-07-08
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_WS_PRIVATE_BOND|˽ļծҵ��¼ģ��
    Ŀ���  :RWA_SMZ_EXPOSURE|˽ļծ���÷��ձ�¶��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'PRO_RWA_SMZ_EXPOSURE';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_SMZ_EXPOSURE';

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1 ���²�¼���ծȯID
    UPDATE RWA.RWA_WS_PRIVATE_BOND T1 set T1.ZQID =
        (SELECT T2.ZQID FROM
           (SELECT ZQMC,p_data_dt_str || 'SMZ' || lpad(min(rownum), 10, '0') as ZQID
                FROM RWA.RWA_WS_PRIVATE_BOND
                WHERE DATADATE = TO_DATE(p_data_dt_str,'yyyymmdd') GROUP BY ZQMC) T2
         WHERE T1.ZQMC = T2.ZQMC
        )
    WHERE T1.DATADATE = TO_DATE(p_data_dt_str,'yyyymmdd')
    ;
    COMMIT;
    --2.2 ˽ļծ-���÷��ձ�¶
    INSERT INTO RWA_SMZ_EXPOSURE(
                DATADATE                               --��������
                ,DATANO                              	 --������ˮ��
                ,EXPOSUREID                          	 --���ձ�¶ID
                ,DUEID                               	 --ծ��ID
                ,SSYSID                              	 --ԴϵͳID
                ,CONTRACTID                          	 --��ͬID
                ,CLIENTID                            	 --��������ID
                ,SORGID                              	 --Դ����ID
                ,SORGNAME                            	 --Դ��������
                ,ORGID                               	 --��������ID
                ,ORGNAME                             	 --������������
                ,ACCORGID                            	 --�������ID
                ,ACCORGNAME                          	 --�����������
                ,INDUSTRYID                          	 --������ҵ����
                ,INDUSTRYNAME                  			 	 --������ҵ����
                ,BUSINESSLINE                        	 --����
                ,ASSETTYPE                           	 --�ʲ�����
                ,ASSETSUBTYPE                        	 --�ʲ�С��
                ,BUSINESSTYPEID                      	 --ҵ��Ʒ�ִ���
                ,BUSINESSTYPENAME                    	 --ҵ��Ʒ������
                ,CREDITRISKDATATYPE                  	 --���÷�����������
                ,ASSETTYPEOFHAIRCUTS                 	 --�ۿ�ϵ����Ӧ�ʲ����
                ,BUSINESSTYPESTD                     	 --Ȩ�ط�ҵ������
                ,EXPOCLASSSTD                        	 --Ȩ�ط���¶����
                ,EXPOSUBCLASSSTD                     	 --Ȩ�ط���¶С��
                ,EXPOCLASSIRB                        	 --��������¶����
                ,EXPOSUBCLASSIRB                     	 --��������¶С��
                ,EXPOBELONG                          	 --��¶������ʶ
                ,BOOKTYPE                            	 --�˻����
                ,REGUTRANTYPE                        	 --��ܽ�������
                ,REPOTRANFLAG                        	 --�ع����ױ�ʶ
                ,REVAFREQUENCY                       	 --�ع�Ƶ��
                ,CURRENCY                            	 --����
                ,NORMALPRINCIPAL                     	 --�����������
                ,OVERDUEBALANCE     				   		   	 --�������
                ,NONACCRUALBALANCE                   	 --��Ӧ�����
                ,ONSHEETBALANCE                      	 --�������
                ,NORMALINTEREST                      	 --������Ϣ
                ,ONDEBITINTEREST                     	 --����ǷϢ
                ,OFFDEBITINTEREST                    	 --����ǷϢ
                ,EXPENSERECEIVABLE                   	 --Ӧ�շ���
                ,ASSETBALANCE                        	 --�ʲ����
                ,ACCSUBJECT1                         	 --��Ŀһ
                ,ACCSUBJECT2                         	 --��Ŀ��
                ,ACCSUBJECT3                         	 --��Ŀ��
                ,STARTDATE                           	 --��ʼ����
                ,DUEDATE                             	 --��������
                ,ORIGINALMATURITY                    	 --ԭʼ����
                ,RESIDUALM                           	 --ʣ������
                ,RISKCLASSIFY                        	 --���շ���
                ,EXPOSURESTATUS                      	 --���ձ�¶״̬
                ,OVERDUEDAYS                         	 --��������
                ,SPECIALPROVISION                    	 --ר��׼����
                ,GENERALPROVISION                    	 --һ��׼����
                ,ESPECIALPROVISION                   	 --�ر�׼����
                ,WRITTENOFFAMOUNT                    	 --�Ѻ������
                ,OFFEXPOSOURCE                       	 --���Ⱪ¶��Դ
                ,OFFBUSINESSTYPE                     	 --����ҵ������
                ,OFFBUSINESSSDVSSTD                  	 --Ȩ�ط�����ҵ������ϸ��
                ,UNCONDCANCELFLAG                    	 --�Ƿ����ʱ����������
                ,CCFLEVEL                            	 --����ת��ϵ������
                ,CCFAIRB                             	 --�߼�������ת��ϵ��
                ,CLAIMSLEVEL                         	 --ծȨ����
                ,BONDFLAG                            	 --�Ƿ�Ϊծȯ
                ,BONDISSUEINTENT                     	 --ծȯ����Ŀ��
                ,NSUREALPROPERTYFLAG                 	 --�Ƿ�����ò�����
                ,REPASSETTERMTYPE                    	 --��ծ�ʲ���������
                ,DEPENDONFPOBFLAG                    	 --�Ƿ�����������δ��ӯ��
                ,IRATING                             	 --�ڲ�����
                ,PD                                  	 --ΥԼ����
                ,LGDLEVEL                               --ΥԼ��ʧ�ʼ���
                ,LGDAIRB                                --�߼���ΥԼ��ʧ��
                ,MAIRB                                  --�߼�����Ч����
                ,EADAIRB                                --�߼���ΥԼ���ձ�¶
                ,DEFAULTFLAG                            --ΥԼ��ʶ
                ,BEEL                                   --��ΥԼ��¶Ԥ����ʧ����
                ,DEFAULTLGD                             --��ΥԼ��¶ΥԼ��ʧ��
                ,EQUITYEXPOFLAG                         --��Ȩ��¶��ʶ
                ,EQUITYINVESTTYPE                       --��ȨͶ�ʶ�������
                ,EQUITYINVESTCAUSE                      --��ȨͶ���γ�ԭ��
                ,SLFLAG                                 --רҵ�����ʶ
                ,SLTYPE                                 --רҵ��������
                ,PFPHASE                                --��Ŀ���ʽ׶�
                ,REGURATING                             --�������
                ,CBRCMPRATINGFLAG                       --������϶������Ƿ��Ϊ����
                ,LARGEFLUCFLAG                          --�Ƿ񲨶��Խϴ�
                ,LIQUEXPOFLAG                           --�Ƿ���������з��ձ�¶
                ,PAYMENTDEALFLAG                        --�Ƿ����Ը�ģʽ
                ,DELAYTRADINGDAYS                       --�ӳٽ�������
                ,SECURITIESFLAG                         --�м�֤ȯ��ʶ
                ,SECUISSUERID                           --֤ȯ������ID
                ,RATINGDURATIONTYPE                     --������������
                ,SECUISSUERATING                        --֤ȯ���еȼ�
                ,SECURESIDUALM                          --֤ȯʣ������
                ,SECUREVAFREQUENCY                      --֤ȯ�ع�Ƶ��
                ,CCPTRANFLAG                            --�Ƿ����뽻�׶�����ؽ���
                ,CCPID                                  --���뽻�׶���ID
                ,QUALCCPFLAG                            --�Ƿ�ϸ����뽻�׶���
                ,BANKROLE                               --���н�ɫ
                ,CLEARINGMETHOD                         --���㷽ʽ
                ,BANKASSETFLAG                          --�Ƿ������ύ�ʲ�
                ,MATCHCONDITIONS                        --�����������
                ,SFTFLAG                                --֤ȯ���ʽ��ױ�ʶ
                ,MASTERNETAGREEFLAG                     --���������Э���ʶ
                ,MASTERNETAGREEID                       --���������Э��ID
                ,SFTTYPE                                --֤ȯ���ʽ�������
                ,SECUOWNERTRANSFLAG                     --֤ȯ����Ȩ�Ƿ�ת��
                ,OTCFLAG                                --�����������߱�ʶ
                ,VALIDNETTINGFLAG                       --��Ч�������Э���ʶ
                ,VALIDNETAGREEMENTID                    --��Ч�������Э��ID
                ,OTCTYPE                                --����������������
                ,DEPOSITRISKPERIOD                      --��֤������ڼ�
                ,MTM                                    --���óɱ�
                ,MTMCURRENCY                            --���óɱ�����
                ,BUYERORSELLER                          --������
                ,QUALROFLAG                             --�ϸ�����ʲ���ʶ
                ,ROISSUERPERFORMFLAG                    --�����ʲ��������Ƿ�����Լ
                ,BUYERINSOLVENCYFLAG                    --���ñ������Ƿ��Ʋ�
                ,NONPAYMENTFEES                         --��δ֧������
                ,RETAILEXPOFLAG                         --���۱�¶��ʶ
                ,RETAILCLAIMTYPE                        --����ծȨ����
                ,MORTGAGETYPE                           --ס����Ѻ��������
                ,DEBTORNUMBER                           --����˸���
                ,EXPONUMBER                             --���ձ�¶����
                ,PDPOOLMODELID                          --PD�ֳ�ģ��ID
                ,LGDPOOLMODELID                         --LGD�ֳ�ģ��ID
                ,CCFPOOLMODELID                         --CCF�ֳ�ģ��ID
                ,PDPOOLID                               --����PD��ID
                ,LGDPOOLID                              --����LGD��ID
                ,CCFPOOLID                              --����CCF��ID
                ,ABSUAFLAG                              --�ʲ�֤ȯ�������ʲ���ʶ
                ,ABSPOOLID                              --֤ȯ���ʲ���ID
                ,ABSPROPORTION                          --�ʲ�֤ȯ������
                ,GROUPID                                --������
                ,ORGSORTNO                             --�������������
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')                       AS DATADATE                 --��������
                ,p_data_dt_str                                          AS DATANO                   --������ˮ��
                ,T1.ZQID                                                AS EXPOSUREID               --���ձ�¶ID
                ,T1.ZQID                                                AS DUEID                     --ծ��ID
                ,'SMZ'                                                  AS SSYSID                   --Դϵͳ����
                ,T1.ZQID                                                AS CONTRACTID               --��ͬID
                ,T1.CUSTID1                                             AS CLIENTID                 --��������ID
                ,T1.YWSSJGDM                                             AS SORGID                   --Դ����ID
                ,T3.ORGNAME                                             AS SORGNAME                 --Դ��������
                ,T1.YWSSJGDM                                            AS ORGID                    --������������
                ,T3.ORGNAME                                             AS ORGNAME                  --������������
                ,T1.YWSSJGDM                                            AS ACCORGID                 --�������ID
                ,T3.ORGNAME                                              AS ACCORGNAME               --�����������
                ,T1.RZZJHYTXDM                                          AS INDUSTRYID               --������ҵ����
                ,T4.ITEMNAME                                             AS INDUSTRYNAME             --������ҵ����
                ,''                                                     AS BUSINESSLINE             --����
                ,''                                                     AS ASSETTYPE                --�ʲ�����
                ,''                                                     AS ASSETSUBTYPE             --�ʲ�С��
                ,'112010'                                               AS BUSINESSTYPEID            --ҵ��Ʒ�ִ���
                ,'˽ļծ'                                               AS BUSINESSTYPENAME         --ҵ��Ʒ������
                ,'06'                                                   AS CREDITRISKDATATYPE       --���÷�����������
                ,'01'                                                   AS ASSETTYPEOFHAIRCUTS      --�ۿ�ϵ����Ӧ�ʲ����
                ,'07'                                                   AS BUSINESSTYPESTD          --Ȩ�ط�ҵ������
                ,''                                                     AS EXPOCLASSSTD             --Ȩ�ط���¶����
                ,''                                                     AS EXPOSUBCLASSSTD           --Ȩ�ط���¶С��
                ,''                                                     AS EXPOCLASSIRB             --��������¶����
                ,''                                                     AS EXPOSUBCLASSIRB           --��������¶С��
                ,'01'                                                   AS EXPOBELONG               --��¶������ʶ
                ,'01'                                                   AS BOOKTYPE                 --�˻����
                ,'03'                                                   AS REGUTRANTYPE             --��ܽ�������
                ,'0'                                                    AS REPOTRANFLAG              --�ع����ױ�ʶ           Ĭ�ϣ�0-��
                ,1                                                      AS REVAFREQUENCY            --�ع�Ƶ��
                ,NVL(T1.YWBZDM,'CNY')                                    AS CURRENCY                 --����
                ,ROUND(TO_NUMBER(T1.YWYE),6)                             AS NORMALPRINCIPAL          --�����������
                ,0                                                      AS OVERDUEBALANCE           --�������
                ,0                                                      AS NONACCRUALBALANCE        --��Ӧ�����
                ,ROUND(TO_NUMBER(T1.YWYE),6)                             AS ONSHEETBALANCE           --�������
                ,0                                                       AS NORMALINTEREST           --������Ϣ
                ,ROUND(TO_NUMBER(T1.YSWSLX),6)                          AS ONDEBITINTEREST          --����ǷϢ
                ,0                                                      AS OFFDEBITINTEREST         --����ǷϢ
                ,T1.YSWSSXF                                             AS EXPENSERECEIVABLE        --Ӧ�շ���
                ,ROUND(TO_NUMBER(T1.YWYE),6) + ROUND(TO_NUMBER(T1.YSWSLX),6) + ROUND(TO_NUMBER(T1.YSWSSXF),6)
                                                                        AS ASSETBALANCE             --�ʲ����
                ,T1.KMDM                                                AS ACCSUBJECT1               --��Ŀһ
                ,''                                                      AS ACCSUBJECT2               --��Ŀ��
                ,''                                                       AS ACCSUBJECT3               --��Ŀ��
                ,REPLACE(T1.ZQFXQSRQ,'-','')                            AS STARTDATE                 --��ʼ����
                ,REPLACE(T1.ZQDQRQ,'-','')                              AS DUEDATE                    --��������
                ,CASE WHEN (to_date(T1.ZQDQRQ,'yyyy-mm-dd') - to_date(T1.ZQFXQSRQ,'yyyy-mm-dd')) / 365<0
                      THEN 0
                      ELSE (to_date(T1.ZQDQRQ,'yyyy-mm-dd') - to_date(T1.ZQFXQSRQ,'yyyy-mm-dd')) / 365
                END                                                      AS ORIGINALMATURITY         --ԭʼ����                  ��λ����
                ,CASE WHEN (to_date(T1.ZQDQRQ,'yyyy-mm-dd') - to_date(p_data_dt_str,'yyyymmdd')) / 365<0
                      THEN 0
                      ELSE (to_date(T1.ZQDQRQ,'yyyy-mm-dd') - to_date(p_data_dt_str,'yyyymmdd')) / 365
                END                                                      AS RESIDUALM                 --ʣ������
                ,'01'                                                    AS RISKCLASSIFY             --���շ���
                ,''                                                     AS EXPOSURESTATUS           --���ձ�¶״̬
                ,0                                                      AS OVERDUEDAYS              --��������
                ,0                                                      AS SPECIALPROVISION         --ר��׼����
                ,0                                                      AS GENERALPROVISION         --һ��׼����
                ,0                                                      AS ESPECIALPROVISION        --�ر�׼����
                ,0                                                      AS WRITTENOFFAMOUNT         --�Ѻ������
                ,''                                                     AS OFFEXPOSOURCE            --���Ⱪ¶��Դ
                ,''                                                     AS OFFBUSINESSTYPE          --����ҵ������
                ,''                                                     AS OFFBUSINESSSDVSSTD       --Ȩ�ط�����ҵ������ϸ��
                ,'0'                                                    AS UNCONDCANCELFLAG         --�Ƿ����ʱ����������
                ,''                                                     AS CCFLEVEL                 --����ת��ϵ������
                ,''                                                     AS CCFAIRB                  --�߼�������ת��ϵ��
                ,'01'                                                   AS CLAIMSLEVEL              --ծȨ����
                ,'0'                                                    AS BONDFLAG                 --�Ƿ�Ϊծȯ
                ,'02'                                                   AS BONDISSUEINTENT          --ծȯ����Ŀ��
                ,'0'                                                    AS NSUREALPROPERTYFLAG      --�Ƿ�����ò�����
                ,'0'                                                    AS REPASSETTERMTYPE         --��ծ�ʲ���������
                ,'0'                                                    AS DEPENDONFPOBFLAG         --�Ƿ�����������δ��ӯ��
                ,''                                                     AS IRATING                  --�ڲ�����
                ,''                                                     AS PD                       --ΥԼ����
                ,''                                                     AS LGDLEVEL                 --ΥԼ��ʧ�ʼ���
                ,''                                                     AS LGDAIRB                  --�߼���ΥԼ��ʧ��
                ,NULL                                                   AS MAIRB                    --�߼�����Ч����
                ,''                                                      AS EADAIRB                  --�߼���ΥԼ���ձ�¶
                ,'0'                                                    AS DEFAULTFLAG              --ΥԼ��ʶ
                ,''                                                     AS BEEL                     --��ΥԼ��¶Ԥ����ʧ����
                ,''                                                     AS DEFAULTLGD               --��ΥԼ��¶ΥԼ��ʧ��
                ,'0'                                                    AS EQUITYEXPOFLAG           --��Ȩ��¶��ʶ
                ,''                                                     AS EQUITYINVESTTYPE         --��ȨͶ�ʶ�������
                ,''                                                     AS EQUITYINVESTCAUSE        --��ȨͶ���γ�ԭ��
                ,'0'                                                    AS SLFLAG                   --רҵ�����ʶ
                ,''                                                     AS SLTYPE                   --רҵ��������
                ,''                                                     AS PFPHASE                   --��Ŀ���ʽ׶�
                ,'01'                                                   AS REGURATING               --�������
                ,'0'                                                    AS CBRCMPRATINGFLAG         --������϶������Ƿ��Ϊ����
                ,'0'                                                    AS LARGEFLUCFLAG            --�Ƿ񲨶��Խϴ�
                ,'0'                                                    AS LIQUEXPOFLAG             --�Ƿ���������з��ձ�¶
                ,'0'                                                    AS PAYMENTDEALFLAG          --�Ƿ����Ը�ģʽ
                ,0                                                      AS DELAYTRADINGDAYS         --�ӳٽ�������
                ,'0'                                                    AS SECURITIESFLAG           --�м�֤ȯ��ʶ
                ,''                                                      AS SECUISSUERID             --֤ȯ������ID
                ,''                                                      AS RATINGDURATIONTYPE       --������������
                ,''                                                      AS SECUISSUERATING          --֤ȯ���еȼ�
                ,0                                                      AS SECURESIDUALM            --֤ȯʣ������
                ,1                                                      AS SECUREVAFREQUENCY        --֤ȯ�ع�Ƶ��
                ,'0'                                                    AS CCPTRANFLAG              --�Ƿ����뽻�׶�����ؽ���
                ,''                                                     AS CCPID                    --���뽻�׶���ID
                ,'0'                                                    AS QUALCCPFLAG              --�Ƿ�ϸ����뽻�׶���
                ,''                                                     AS BANKROLE                 --���н�ɫ
                ,''                                                     AS CLEARINGMETHOD           --���㷽ʽ
                ,'0'                                                    AS BANKASSETFLAG            --�Ƿ������ύ�ʲ�
                ,''                                                     AS MATCHCONDITIONS          --�����������
                ,'0'                                                    AS SFTFLAG                  --֤ȯ���ʽ��ױ�ʶ
                ,'0'                                                    AS MASTERNETAGREEFLAG       --���������Э���ʶ
                ,''                                                     AS MASTERNETAGREEID         --���������Э��ID
                ,''                                                     AS SFTTYPE                  --֤ȯ���ʽ�������
                ,'0'                                                    AS SECUOWNERTRANSFLAG       --֤ȯ����Ȩ�Ƿ�ת��
                ,'0'                                                    AS OTCFLAG                  --�����������߱�ʶ
                ,'0'                                                    AS VALIDNETTINGFLAG         --��Ч�������Э���ʶ
                ,''                                                     AS VALIDNETAGREEMENTID      --��Ч�������Э��ID
                ,''                                                     AS OTCTYPE                  --����������������
                ,0                                                      AS DEPOSITRISKPERIOD        --��֤������ڼ�
                ,0                                                      AS MTM                      --���óɱ�
                ,''                                                     AS MTMCURRENCY              --���óɱ�����
                ,''                                                     AS BUYERORSELLER            --������
                ,'0'                                                    AS QUALROFLAG               --�ϸ�����ʲ���ʶ
                ,'0'                                                    AS ROISSUERPERFORMFLAG      --�����ʲ��������Ƿ�����Լ
                ,'0'                                                    AS BUYERINSOLVENCYFLAG      --���ñ������Ƿ��Ʋ�
                ,0                                                      AS NONPAYMENTFEES           --��δ֧������
                ,'0'                                                    AS RETAILEXPOFLAG           --���۱�¶��ʶ                   Ĭ�ϣ�0-��
                ,''                                                     AS RETAILCLAIMTYPE          --����ծȨ����
                ,'0'                                                    AS MORTGAGETYPE             --ס����Ѻ��������
                ,0                                                      AS DEBTORNUMBER             --����˸���
                ,1                                                      AS EXPONUMBER               --���ձ�¶����
                ,''                                                     AS PDPOOLMODELID            --PD�ֳ�ģ��ID
                ,''                                                     AS LGDPOOLMODELID           --LGD�ֳ�ģ��ID
                ,''                                                     AS CCFPOOLMODELID           --CCF�ֳ�ģ��ID
                ,''                                                     AS PDPOOLID                 --����PD��ID
                ,''                                                     AS LGDPOOLID                --����LGD��ID
                ,''                                                     AS CCFPOOLID                --����CCF��ID
                ,'0'                                                    AS ABSUAFLAG                --�ʲ�֤ȯ�������ʲ���ʶ
                ,''                                                     AS ABSPOOLID                --֤ȯ���ʲ���ID
                ,0                                                      AS ABSPROPORTION            --�ʲ�֤ȯ������
                ,''                                                     AS GROUPID                  --������
                ,T3.SORTNO                                              AS ORGSORTNO                --�������������

    FROM        RWA.RWA_WS_PRIVATE_BOND T1
    INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT T5
    ON          T1.SUPPORGID=T5.ORGID
    AND         T5.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND         T5.SUPPTMPLID='M-0110'
    AND         T5.SUBMITFLAG='1'
    LEFT  JOIN   RWA.ORG_INFO T3
    ON           T1.YWSSJGDM = T3.ORGID
    LEFT  JOIN   RWA.CODE_LIBRARY T4
    ON           T1.RZZJHYTXDM = T4.ITEMNO
    AND         T4.CODENO = 'IndustryType'
    WHERE       T1.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND         T1.ROWID IN(
                    SELECT MAX(BOND.ROWID)
                      FROM RWA.RWA_WS_PRIVATE_BOND BOND
                INNER JOIN RWA.RWA_WP_DATASUPPLEMENT T5
                        ON BOND.SUPPORGID=T5.ORGID
                       AND T5.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
                       AND T5.SUPPTMPLID='M-0110'
                       AND T5.SUBMITFLAG='1'
                     WHERE BOND.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
                  GROUP BY BOND.ZQID
                )
    ;
    COMMIT;


    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_SMZ_EXPOSURE;
    --Dbms_output.Put_line('RWA_SMZ_EXPOSURE��ǰ��������ÿ�ϵͳ���ݼ�¼Ϊ: ' ||TO_CHAR( v_count1 - v_count) || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '˽ļծ-���÷��ձ�¶('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_SMZ_EXPOSURE;
/

