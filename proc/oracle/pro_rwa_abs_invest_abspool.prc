CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ABS_INVEST_ABSPOOL(p_data_dt_str  IN  VARCHAR2, --��������
                                                       p_po_rtncode   OUT VARCHAR2, --���ر��
                                                       p_po_rtnmsg    OUT VARCHAR2  --��������
)
  /*
    �洢��������:RWA_DEV.PRO_RWA_ABS_INVEST_ABSPOOL
    ʵ�ֹ���:�������Ϣȫ������RWA�ӿڱ�Ͷ�ʻ���-�ʲ�֤ȯ����Լ�����Ϣ����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :QHJIANG
    ��дʱ��:2016-06-23
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA.RWA_WS_ABS_INVEST_EXPOSURE|�ʲ�֤ȯ��-Ͷ�ʻ���-���ձ�¶��¼��
    Դ  ��2 :RWA.RWA_WP_DATASUPPLEMENT|��¼������ϸ��Ϣ��
    Դ  ��3 :RWA.ORG_INFO|������
    Ŀ���1 :RWA_DEV.RWA_ABS_INVEST_ABSPOOL|Ͷ�ʻ���-�ʲ�֤ȯ����Լ�����Ϣ��
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */

  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ABS_INVEST_ABSPOOL';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count1 INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ABS_INVEST_ABSPOOL';

    --DBMS_OUTPUT.PUT_LINE('��ʼ�����롾Ͷ�ʻ���-�ʲ�֤ȯ����Լ�����Ϣ��' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    INSERT INTO RWA_DEV.RWA_ABS_INVEST_ABSPOOL(
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
    SELECT
            ABSR.DATADATE                      AS DATADATE           --��������
           ,TO_CHAR(ABSR.DATADATE,'YYYYMMDD')  AS DATANO             --������ˮ��_�ʲ�֤ȯ��Ͷ����Ϣ��.��������
           ,ABSR.ZCCDH                         AS ABSPOOLID          --֤ȯ���ʲ��ش���_�ʲ�֤ȯ��Ͷ����Ϣ��.�ʲ��ش���
           ,ABSR.ZQHFQRZZJGDM                  AS ABSORIGINATORID    --֤ȯ�������˴���_�ʲ�֤ȯ��Ͷ����Ϣ��.֤ȯ��������֯��������
           ,OI.SORTNO                          AS ORGSORTNO          --�������������
           ,ABSR.YWSSJG                        AS ORGID              --������������_�ʲ�֤ȯ��Ͷ����Ϣ��.��������
           ,OI.ORGNAME                         AS ORGNAME            --��������
           ,ABSR.TX                            AS BUSINESSLINE       --����
           ,'310'                              AS ASSETTYPE          --�ʲ�����
           ,'31001'                            AS ASSETSUBTYPE       --�ʲ�С��
           ,ABSR.ZCZQHMC                       AS ABSNAME            --�ʲ�֤ȯ������_�ʲ�֤ȯ��Ͷ����Ϣ��.�ʲ�֤ȯ������
           ,ABSR.ZCZQHLX                       AS ABSTYPE            --�ʲ�֤ȯ������_�ʲ�֤ȯ��Ͷ����Ϣ��.�ʲ�֤ȯ������
           ,ABSR.JCZCYWLX                      AS UNDERASSETTYPE     --�����ʲ�����_�ʲ�֤ȯ��Ͷ����Ϣ��.�����ʲ�����
           ,ABSR.ZZCZQHBZ                      AS REABSFLAG          --���ʲ�֤ȯ����ʶ
           ,'0'                  							 AS ORIGINATORFLAG     --�Ƿ������(�����¶������һ��Ϊ���������Ϊ �ǣ�����Ϊ ��)
           ,ABSR.FQJGSFNPFYH                   AS IRBBANKFLAG        --��������Ƿ�����������_�ʲ�֤ȯ��Ͷ����Ϣ��.��������Ƿ�����������
           ,'1'                                AS SATISFYMANAGEFLAG  --�Ƿ���Ϲ�������(Ĭ��Ϊ��(Ĭ�ϣ���   1 �� 0 ��))
           ,'1'                                AS COMPLIANCEABSFLAG  --�Ƿ�Ϲ�֤ȯ��(Ĭ��Ϊ��(Ĭ�ϣ���   1 �� 0 ��))
           ,'0'                                AS PROVIDEISFLAG      --�Ƿ��ṩ����֧��(Ĭ��Ϊ��(Ĭ�ϣ���   1 �� 0 ��))
           ,CASE WHEN ABSR.YHJS = '01' THEN 0
                 ELSE NULL
            END                 							 AS SALEGAINS          --��������_��������
           ,NULL															 AS PROPUNDERASSETIRB  --�����ʲ������������������
           ,'0'                                AS SIMPLALGOFLAG      --�Ƿ���ü򻯷���(Ĭ��Ϊ��(Ĭ�ϣ���   1 �� 0 ��))
           ,0.03                               AS LARGESTEXPOPP      --�����ձ�¶���ʲ���Ϸݶ�_����ʲ�֤ȯ�����ձ�¶�ݶ�
           ,NULL															 AS N									 --��Ч����

    FROM 				RWA.RWA_WS_ABS_INVEST_EXPOSURE ABSR
    INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT RWD
    ON          ABSR.SUPPORGID = RWD.ORGID
    AND         RWD.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND         RWD.SUPPTMPLID = 'M-0140'
    AND         RWD.SUBMITFLAG = '1'
    LEFT JOIN   RWA.ORG_INFO OI                                       --������
    ON          ABSR.YWSSJG = OI.ORGID
    WHERE       ABSR.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND         ABSR.YHJS='02'																				--02 Ͷ�ʻ��� ����ȫ�ŵ����л�����¶��
    ;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ABS_INVEST_ABSPOOL',cascade => true);

    --DBMS_OUTPUT.PUT_LINE('���������롾Ͷ�ʻ���-�ʲ�֤ȯ����Լ�����Ϣ��' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_ABS_INVEST_ABSPOOL;
    --DBMS_OUTPUT.PUT_LINE('RWA_DEV.RWA_ABS_INVEST_ABSPOOL-Ͷ�ʻ���-�ʲ�֤ȯ����Լ�����Ϣ���в�������Ϊ��' || v_count1 || '��');

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    P_PO_RTNMSG  := '�ɹ�'||'-'||v_count1;

    --�����쳣
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '���롾Ͷ�ʻ���-�ʲ�֤ȯ����Լ�����Ϣ��('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_ABS_INVEST_ABSPOOL;
/

