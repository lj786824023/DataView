CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ABS_INVEST_ABSEXPOSURE(p_data_dt_str  IN  VARCHAR2, --��������
                                                           p_po_rtncode   OUT VARCHAR2, --���ر��
                                                           p_po_rtnmsg    OUT VARCHAR2  --��������
)
  /*
    �洢��������:RWA_DEV.PRO_RWA_ABS_INVEST_ABSEXPOSURE
    ʵ�ֹ���:�������Ϣȫ������RWA�ӿڱ��ʲ�֤ȯ��Ͷ�ʻ������ձ�¶����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :QHJIANG
    ��дʱ��:2016-06-23
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA.RWA_WS_ABS_INVEST_EXPOSURE|�ʲ�֤ȯ��-Ͷ�ʻ���-���ձ�¶��¼��
    Դ  ��1 :RWA.RWA_WP_DATASUPPLEMENT|��¼������ϸ��Ϣ��
    Ŀ���1 :RWA_DEV.RWA_ABS_INVEST_ABSEXPOSURE|Ͷ�ʻ���-�ʲ�֤ȯ�����ձ�¶��
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */

  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ABS_INVEST_ABSEXPOSURE';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count1 INTEGER;
  v_count2 INTEGER;
  v_count3 INTEGER;


  BEGIN

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ABS_INVEST_ABSEXPOSURE';


    --DBMS_OUTPUT.PUT_LINE('��ʼ�����롾�ʲ�֤ȯ��Ͷ�ʻ������ձ�¶��' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    INSERT INTO RWA_DEV.RWA_ABS_INVEST_ABSEXPOSURE(
             DataDate            --��������
            ,DataNo              --������ˮ��
            ,ABSExposureID       --�ʲ�֤ȯ�����ձ�¶ID
            ,ABSPoolID           --֤ȯ���ʲ���ID
            ,ABSOriginatorID     --֤ȯ��������ID
            ,OrgSortNo           --�������������
            ,OrgID               --��������ID
            ,OrgName             --������������
            ,BusinessLine        --ҵ������
            ,AssetType           --�ʲ�����
            ,AssetSubType        --�ʲ�С��
            ,ExpoCategoryIRB     --��������¶���
            ,ExpoBelong          --��¶������ʶ
            ,BookType            --�˻����
            ,AssetTypeOfHaircuts --�ۿ�ϵ����Ӧ�ʲ����
            ,ReABSFlag           --���ʲ�֤ȯ����ʶ
            ,RetailFlag          --���۱�ʶ
            ,ReguTranType        --��ܽ�������
            ,RevaFrequency       --�ع�Ƶ��
            ,OffABSBusinessType  --�����ʲ�֤ȯ��ҵ������
            ,ABSRole             --�ʲ�֤ȯ����ɫ
            ,ProvideCSRERFlag    --�Ƿ��ṩ����֧�ֲ���ӳ���ⲿ����
            ,ProvideCRMFSPEFlag  --�Ƿ��ṩ���÷��ջ��͸��ر�Ŀ�Ļ���
            ,MitiProvMERating    --�����ṩ�߻���ʱ�ⲿ����
            ,MitiProvCERating    --�����ṩ�ߵ�ǰ�ⲿ����
            ,QualFaciFlag        --�ϸ������ʶ
            ,UncondCancelFlag    --�Ƿ����ʱ����������
            ,TrenchSN            --�ֵ�˳���
            ,TrenchName          --��������
            ,TopTrenchFlag       --�Ƿ���ߵ���
            ,PreferedTrenchFlag  --�Ƿ������ȵ���
            ,RatingDurationType  --������������
            ,ERatingResult       --�ⲿ�������
            ,InferRatingResult   --�Ʋ��������
            ,IssueDate           --��������
            ,DueDate             --��������
            ,OriginalMaturity    --ԭʼ����
            ,ResidualM           --ʣ������
            ,AssetBalance        --�ʲ����
            ,Currency            --����
            ,Provisions          --��ֵ׼��
            ,L                   --������������ˮƽ
            ,T                   --���κ��
            ,EarlyAmortType      --��ǰ̯������
            ,RetailCommitType    --���۳�ŵ����
            ,AverIGOnThreeMths   --������ƽ����������
            ,IntGapStopPoint     --��������������
            ,R                   --������ƽ����������/������
            ,ISSUSERASSETPROP    --���л��������ʲ�ռ��
            ,INVESTOR            --Ͷ����Ȩ��
    )
    SELECT
            ABSR.DATADATE                                                             AS DATADATE            --��������
           ,TO_CHAR(ABSR.DATADATE,'YYYYMMDD')                                         AS DATANO              --������ˮ��_��������
           ,'XN_'||ABSR.SUPPSERIALNO                                                  AS ABSEXPOSUREID       --�ʲ�֤ȯ�����ձ�¶����_��ˮ��('XN_'||'ZCZQH'||��ˮ��)
           ,ABSR.ZCCDH                                                                AS ABSPOOLID           --֤ȯ���ʲ��ش���_�ʲ��ش���
           ,ABSR.ZQHFQRZZJGDM                                                         AS ABSORIGINATORID     --֤ȯ�������˴���_֤ȯ����������֯��������
           ,OI.SORTNO                                                                 AS ORGSORTNO           --�������������
           ,ABSR.YWSSJG                                                               AS ORGID               --��������_��������
           ,OI.ORGNAME                                                                AS ORGNAME             --������������
           ,ABSR.TX                                                                   AS BUSINESSLINE        --����_����
           ,'310'                                                                     AS ASSETTYPE           --�ʲ�����_(Ĭ��Ϊ�ʲ�֤ȯ�� ��ֵ��AssetCategory)
           ,'31001'                                                                   AS ASSETSUBTYPE        --�ʲ�С��_(Ĭ��Ϊ�ʲ�֤ȯ�� ��ֵ��AssetCategory)
           ,CASE WHEN ABSR.YHJS IN ('01','02') THEN '020601'
                 WHEN ABSR.YHJS = '03' THEN '020602'
                 WHEN ABSR.YHJS = '04' THEN '020603'
                 WHEN ABSR.YHJS = '05' THEN '020604'
                 ELSE ''
            END                                            														AS EXPOCATEGORYIRB     --���ձ�¶���_(��ֵ��ExpoCategoryIRB)
           ,CASE WHEN ABSR.YHJS IN ('03','04','05') THEN '02'
                 ELSE '01'
            END                                              													AS EXPOBELONG          --��¶������ʶ_��¶������ʶ��  �������ʲ�֤ȯ������Ϊ01��02��03��04����¶��ʶΪ02-���⣬����Ϊ01-����
           ,CASE WHEN FBI.ASSET_CLASS = '10' THEN '02'
                 ELSE '01'
            END													                                              AS BOOKTYPE            --�˻����
           ,'21907'                                                                   AS SETTYPEOFHAIRCUTS   --�ۿ�ϵ����Ӧ�ʲ����� Ĭ��Ϊ���� �ʲ����� AssetCategory �еġ�21907 ����
           ,ABSR.ZZCZQHBZ                                                             AS REABSFLAG           --���ʲ�֤ȯ����ʶ
           ,CASE WHEN ABSR.JCZCYWLX IN ('02','03','04')
                 THEN '1'
                 ELSE '0'
            END							                                                          AS RETAILFLAG          --���۱�ʶ_�����ʲ�����("�������ʲ�ҵ������Ϊ 01-����ס�������ʲ�֤ȯ�������ʲ� 02-�������÷������ʲ�֤ȯ�������ʲ� 03-�������Ѵ����ʲ�֤ȯ�������ʲ� 04-���˶�ȴ����ʲ�֤ȯ�������ʲ� 05-���˾�Ӫ�����ʲ�֤ȯ�������ʲ� 06-������ѧ�����ʲ�֤ȯ�������ʲ� 07-�������������ʲ�֤ȯ�������ʲ� �����۱�ʶ��Ϊ�ǣ� �������۱�ʶ��Ϊ�� (1 �� 0 ��)")
           ,'02'                                                                      AS REGUTRANTYPE        --��ܽ�������_("Ĭ��Ϊ02-�����ʱ��г����� ��ֵ��ReguTranType")
           ,'1'                                                                       AS REVAFREQUENCY       --�ع�Ƶ��_(Ĭ��Ϊ1��)
           ,CASE WHEN ABSR.YHJS = '03' THEN '04'
                 WHEN ABSR.YHJS = '04' THEN '01'
                 WHEN ABSR.YHJS = '05' THEN '02'
                 ELSE ''
            END                                                 											AS OFFABSBUSINESSTYPE  --�����ʲ�֤ȯ������
           ,ABSR.YHJS                                                                 AS ABSROLE             --�ʲ�֤ȯ����ɫ
           ,ABSR.SFTGXYZCBFYDWBPJ                                                     AS PROVIDECSRERFLAG    --�Ƿ��ṩ����֧�ֲ���ӳ���ⲿ����
           ,ABSR.XYFXHSSFTGGTBMDDJG                                                   AS PROVIDECRMFSPEFLAG  --�Ƿ��ṩ���÷��ջ��͸��ر�Ŀ�Ļ���
           ,CASE WHEN ABSR.FQSHSTGZWBPJ IS NULL THEN '0124'
           	ELSE RWA_DEV.GETSTANDARDRATING1(ABSR.FQSHSTGZWBPJ)
           	END																					                              AS MITIPROVMERATING    --�ṩ����ʱ�ⲿ����
           ,CASE WHEN ABSR.DQHSTGZWBPJ IS NULL THEN '0124'
           	ELSE RWA_DEV.GETSTANDARDRATING1(ABSR.DQHSTGZWBPJ)
           	END																					                              AS MITIPROVCERATING    --��ǰ�ⲿ����
           ,'0'                                                                       AS QUALFACIFLAG        --�ϸ������ʶ Ĭ�Ϸ�
           ,'0'                                                                       AS UNCONDCANCELFLAG    --�Ƿ����ʱ����������_Ĭ�Ϸ�
           ,ABSR.FDSXH                                                                AS TRENCHSN            --�ֵ�˳���_�ֵ�˳���
           ,ABSR.DCMC                                                                 AS TRENCHNAME          --��������_��������
           ,ABSR.SFZYXDC                                                              AS TOPTRENCHFLAG       --�Ƿ���ߵ���_�Ƿ������ȵ���
           ,ABSR.SFZYXDC                                                              AS PREFEREDTRENCHFLAG  --�Ƿ������ȵ���_�Ƿ������ȵ���
           ,CASE WHEN ABSR.ZQWBPJDJ IS NULL THEN '01'
           	ELSE NVL(ABSR.ZQWBPJQX,SUBSTR(RWA_DEV.GETSTANDARDRATING1(ABSR.ZQWBPJDJ),1,2))
           	END					                                                              AS RATINGDURATIONTYPE  --������������_ծȯ�ⲿ��������
           ,CASE WHEN ABSR.ZQWBPJDJ IS NULL THEN '0124'
           	ELSE RWA_DEV.GETSTANDARDRATING1(ABSR.ZQWBPJDJ)
           	END																			                                  AS ERATINGRESULT       --�ⲿ�������_ծȯ�ⲿ�����ȼ�
           ,'0124'                                                                    AS INFERRATINGRESULT   --�Ʋ��������_(Ĭ��Ϊδ���� 0124 δ����)
           ,TO_CHAR(TO_DATE(ABSR.FXR,'YYYY-MM-DD'),'YYYYMMDD')                        AS ISSUEDATE           --��������_������
           ,TO_CHAR(TO_DATE(ABSR.DQR,'YYYY-MM-DD'),'YYYYMMDD')                        AS DUEDATE             --��������_������
           ,CASE WHEN (TO_DATE(ABSR.DQR,'YYYY-MM-DD')-TO_DATE(ABSR.FXR,'YYYY-MM-DD')) / 365 < 0
                 THEN 0
                 ELSE (TO_DATE(ABSR.DQR,'YYYY-MM-DD')-TO_DATE(ABSR.FXR,'YYYY-MM-DD')) / 365
            END                                                                       AS ORIGINALMATURITY    --ԭʼ����_"������ ������"("������-����������Ϊ��λ")
           ,CASE WHEN (TO_DATE(ABSR.DQR,'YYYY-MM-DD')-TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0
                 THEN 0
                 ELSE (TO_DATE(ABSR.DQR,'YYYY-MM-DD')-TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
            END                                                                       AS RESIDUALM           --ʣ������_������("������-��ǰ�� ����Ϊ��λ")
           ,ROUND(TO_NUMBER(REPLACE(NVL(ABSR.ZCYE,'0'),',','')),6)                    AS ASSETBALANCE        --�ʲ����_�ʲ����
           ,NVL(ABSR.BZ,'CNY')                                                        AS CURRENCY            --����_����
           ,ROUND(TO_NUMBER(REPLACE(NVL(ABSR.JZZB,'0'),',','')),6)                    AS PROVISIONS          --��ֵ׼��_��ֵ׼��
           ,0                                                                         AS L                   --������������ˮƽ(Ĭ�� 0)
           ,0                                                                         AS T                   --���κ��(Ĭ�� 0)
           ,''                                                                        AS EARLYAMORTTYPE      --��ǰ̯������(Ĭ�� ��)
           ,''                                                                        AS RETAILCOMMITTYPE    --���۳�ŵ����(Ĭ�� ��)
           ,0                                                                         AS AVERIGONTHREEMTHS   --������ƽ����������(Ĭ�� 0)
           ,0                                                                         AS INTGAPSTOPPOINT     --��������������(Ĭ�� 0)
           ,0                                                                         AS R                   --������ƽ����������/������_������ƽ����������/��������������(Ĭ�� 0)
           ,NULL																																			AS ISSUSERASSETPROP    --���л��������ʲ�ռ��
           ,NULL                                                                      AS INVESTOR            --Ͷ����Ȩ��(Ĭ�� 0)

    FROM 			 	RWA.RWA_WS_ABS_INVEST_EXPOSURE ABSR
    INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT RWD
    ON          ABSR.SUPPORGID = RWD.ORGID
    AND         RWD.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND         RWD.SUPPTMPLID = 'M-0140'
    AND         RWD.SUBMITFLAG = '1'
    LEFT JOIN   RWA.ORG_INFO OI                                       --������
    ON          ABSR.YWSSJG = OI.ORGID
    LEFT JOIN   RWA_DEV.FNS_BND_INFO_B FBI                            --ծȯ��Ϣ��
    ON          FBI.BOND_ID = ABSR.ZQNM
    AND         FBI.DATANO = p_data_dt_str
    WHERE 			ABSR.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND   			ABSR.YHJS = '02'																			--02 Ͷ�ʻ��� ����ȫ�ŵ����л�����¶��
    ;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ABS_INVEST_ABSEXPOSURE',cascade => true);

    --DBMS_OUTPUT.PUT_LINE('���������롾�ʲ�֤ȯ��Ͷ�ʻ������ձ�¶��' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count1 FROM RWA_ABS_INVEST_ABSEXPOSURE;
    --DBMS_OUTPUT.PUT_LINE('RWA_ABS_INVEST_ABSEXPOSURE-�ʲ�֤ȯ��Ͷ�ʻ������ձ�¶���в�������Ϊ��' || v_count1 || '��');

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count1;

    commit;
    --�����쳣
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '���롾�ʲ�֤ȯ��Ͷ�ʻ������ձ�¶��('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_ABS_INVEST_ABSEXPOSURE;
/

