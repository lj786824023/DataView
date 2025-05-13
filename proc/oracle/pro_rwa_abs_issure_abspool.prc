CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ABS_ISSURE_ABSPOOL(p_data_dt_str  IN  VARCHAR2, --��������
                                                       p_po_rtncode   OUT VARCHAR2, --���ر��
                                                       p_po_rtnmsg    OUT VARCHAR2  --��������
)
  /*
    �洢��������:RWA_DEV.PRO_RWA_ABS_ISSURE_ABSPOOL
    ʵ�ֹ���:�������Ϣȫ������RWA�ӿڱ��л���-�ʲ�֤ȯ����Լ�����Ϣ����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :QHJIANG
    ��дʱ��:2016-06-23
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA.RWA_WS_ABS_ISSUE_POOL|�ʲ�֤ȯ��-���л���-��Լ��ز�¼��
    Դ  ��2 :RWA.ORG_INFO|������
    Դ  ��3 :RWA.RWA_WP_DATASUPPLEMENT|��¼������ϸ��Ϣ��
    Ŀ���1 :RWA_DEV.RWA_ABS_ISSURE_ABSPOOL|���л���-�ʲ�֤ȯ����Լ�����Ϣ��
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */

  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ABS_ISSURE_ABSPOOL';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count1 INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ABS_ISSURE_ABSPOOL';

    --DBMS_OUTPUT.PUT_LINE('��ʼ�����롾���л���-�ʲ�֤ȯ����Լ�����Ϣ��' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    INSERT INTO RWA_DEV.RWA_ABS_ISSURE_ABSPOOL(
         DataDate           	--��������
        ,DataNo             	--������ˮ��
        ,ABSPoolID          	--֤ȯ���ʲ���ID
        ,ABSOriginatorID    	--֤ȯ��������ID
        ,OrgSortNo          	--�������������
        ,OrgID              	--��������ID
        ,OrgName            	--������������
        ,BusinessLine       	--ҵ������
        ,AssetType          	--�ʲ�����
        ,AssetSubType       	--�ʲ�С��
        ,ABSName            	--�ʲ�֤ȯ������
        ,ABSType            	--�ʲ�֤ȯ������
        ,UnderAssetType     	--�����ʲ�����
        ,ReABSFlag          	--���ʲ�֤ȯ����ʶ
        ,OriginatorFlag     	--�Ƿ������
        ,IRBBankFlag        	--��������Ƿ�����������
        ,SatisfyManageFlag   	--�Ƿ���Ϲ�������
        ,ComplianceABSFlag   	--�Ƿ�Ϲ��ʲ�֤ȯ��
        ,ProvideISFlag       	--�Ƿ��ṩ����֧��
        ,SaleGains           	--��������
        ,PropUnderAssetIRB   	--�����ʲ������������������
        ,SimplAlgoFlag       	--�Ƿ���ü򻯷���
        ,LargestExpoPP       	--�����ձ�¶���ʲ���Ϸݶ�
        ,N                    --��Ч����
    )
    WITH TEMP_TABLE AS (
          SELECT  		ZCCBH                                               AS ZCCBH          --�ʲ��ش���
		                 ,SUM(CASE WHEN RWAIE.YHJS='01' THEN 1 ELSE 0 END)    AS ORIGINATORFLAG --�Ƿ������
		                 ,MAX(RWAIE.TX)                                       AS BUSINESSLINE   --����
		                 ,MAX(RWAIE.ZZCZQHBZ)                                 AS REABSFLAG      --���ʲ�֤ȯ����ʶ
          FROM 				RWA.RWA_WS_ABS_ISSUE_EXPOSURE RWAIE
          INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT RWD
          ON          RWAIE.SUPPORGID=RWD.ORGID
          AND         RWD.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
          AND         RWD.SUPPTMPLID='M-0131'
          AND         RWD.SUBMITFLAG='1'
          WHERE				RWAIE.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
          GROUP BY    RWAIE.ZCCBH
    )
    SELECT
				         RWAIP.DATADATE                           AS DATADATE          --��������_��������
				        ,TO_CHAR(RWAIP.DATADATE,'YYYYMMDD')       AS DATANO            --������ˮ��_��������
				        ,RWAIP.ZCCBH                              AS ABSPOOLID         --֤ȯ���ʲ��ش���_�ʲ��ش���
				        ,RWAIP.ZQHFQRZZJGDM                       AS ABSORIGINATORID   --֤ȯ�������˴���_֤ȯ��������֯��������
				        ,OI.SORTNO                                AS ORGSORTNO         --�������������
				        ,RWAIP.YWSSJG                             AS ORGID             --������������_��������
				        ,OI.ORGNAME                               AS ORGNAME           --��������
				        ,NVL(TT.BUSINESSLINE,'0401')              AS BUSINESSLINE      --����
				        ,'310'                                    AS ASSETTYPE         --�ʲ�����
				        ,'31001'                                  AS ASSETSUBTYPE      --�ʲ�С��
				        ,RWAIP.ZCZQHMC                            AS ABSNAME           --�ʲ�֤ȯ������_�ʲ�֤ȯ������
				        ,RWAIP.ZCZQHLX                            AS ABSTYPE           --�ʲ�֤ȯ������_��¼���ݱ���Ĭ�ϣ�ABSType 01 ��ͳ��
				        ,RWAIP.JCZCYWLX                           AS UNDERASSETTYPE    --�����ʲ�����_�����ʲ�ҵ������
				        ,NVL(TT.REABSFLAG,'0')                    AS REABSFLAG         --���ʲ�֤ȯ����ʶ
				        ,NVL(CASE WHEN TT.ORIGINATORFLAG > 0
				                  THEN '1' ELSE '0' END,'0')      AS ORIGINATORFLAG    --�Ƿ������(�����¶������һ��Ϊ���������Ϊ �ǣ�����Ϊ ��)
				        ,'1'                                      AS IRBBANKFLAG       --��������Ƿ�����������
				        ,RWAIP.SFFHGLTJ                           AS SATISFYMANAGEFLAG --�Ƿ���Ϲ�������_�Ƿ���Ϲ�������
				        ,RWAIP.SFHGZCZQH                          AS COMPLIANCEABSFLAG --�Ƿ�Ϲ�֤ȯ��_�Ƿ�Ϲ��ʲ�֤ȯ��
				        ,RWAIP.SFTGYXZC                           AS PROVIDEISFLAG     --�Ƿ��ṩ����֧��_�Ƿ��ṩ����֧��
				        ,CASE WHEN TT.ORIGINATORFLAG > 0 THEN NVL(RWAIP.XSLD,0)
				              ELSE RWAIP.XSLD
				         END										                  AS SALEGAINS         --��������_��������
				        ,NULL                                     AS PROPUNDERASSETIRB --�����ʲ������������������_Ĭ��Ϊ��
				        ,'0'                                      AS SIMPLALGOFLAG     --�Ƿ���ü򻯷���_��Ĭ�� �� 1 �� 0 ��
				        ,0.03                                     AS LARGESTEXPOPP     --�����ձ�¶���ʲ���Ϸݶ�_Ĭ��0.03
				        ,NULL																			AS N								 --��Ч����

   	FROM 				RWA.RWA_WS_ABS_ISSUE_POOL RWAIP
   	INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT RWD
   	ON          RWAIP.SUPPORGID=RWD.ORGID
   	AND         RWD.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
   	AND         RWD.SUPPTMPLID='M-0132'
   	AND         RWD.SUBMITFLAG='1'
   	INNER JOIN  TEMP_TABLE TT
   	ON          TT.ZCCBH = RWAIP.ZCCBH
   	LEFT JOIN   RWA.ORG_INFO OI
   	ON          RWAIP.YWSSJG = OI.ORGID
   	WHERE 			RWAIP.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
   	;

   	COMMIT;

   	dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ABS_ISSURE_ABSPOOL',cascade => true);

    --DBMS_OUTPUT.PUT_LINE('���������롾���л���-�ʲ�֤ȯ����Լ�����Ϣ��' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_ABS_ISSURE_ABSPOOL;
    --DBMS_OUTPUT.PUT_LINE('RWA_DEV.RWA_ABS_ISSURE_ABSPOOL-���л���-�ʲ�֤ȯ����Լ�����Ϣ���в�������Ϊ��' || v_count1 || '��');

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count1;

    --�����쳣
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '���롾���л���-�ʲ�֤ȯ����Լ�����Ϣ��('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_ABS_ISSURE_ABSPOOL;
/

