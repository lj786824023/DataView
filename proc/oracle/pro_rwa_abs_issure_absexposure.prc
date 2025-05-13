CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ABS_ISSURE_ABSEXPOSURE(p_data_dt_str  IN  VARCHAR2, --��������
                                                           p_po_rtncode   OUT VARCHAR2, --���ر��
                                                           p_po_rtnmsg    OUT VARCHAR2  --��������
)
  /*
    �洢��������:RWA_DEV.PRO_RWA_ABS_ISSURE_ABSEXPOSURE
    ʵ�ֹ���:�������Ϣȫ������RWA�ӿڱ��ʲ�֤ȯ�����л������ձ�¶����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :QHJIANG
    ��дʱ��:2016-06-23
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA.RWA_WS_ABS_ISSUE_EXPOSURE|�ʲ�֤ȯ��-���л���-���ձ�¶��¼��
    Դ  ��2 :RWA.RWA_WS_ABS_ISSUE_POOL|�ʲ�֤ȯ��-���л���-��Լ��ز�¼��
    Դ  ��3 :RWA.ORG_INFO|������
    Դ  ��4 :RWA.RWA_WP_DATASUPPLEMENT|��¼������ϸ��Ϣ��
    Ŀ���1 :RWA_ABS_ISSURE_ABSEXPOSURE|���л���-�ʲ�֤ȯ�����ձ�¶��
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */

  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ABS_ISSURE_ABSEXPOSURE';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count1 INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ABS_ISSURE_ABSEXPOSURE';


    --DBMS_OUTPUT.PUT_LINE('��ʼ�����롾�ʲ�֤ȯ�����л������ձ�¶��' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    INSERT INTO RWA_DEV.RWA_ABS_ISSURE_ABSEXPOSURE(
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
				         RWAIE.DATADATE                                                 AS DATADATE            --��������_��������
				        ,TO_CHAR(RWAIE.DATADATE,'yyyyMMdd')                             AS DATANO              --������ˮ��_��������
				        ,'ABS'||RWAIE.SUPPSERIALNO                                      AS ABSEXPOSUREID       --�ʲ�֤ȯ�����ձ�¶����_��ˮ��('BL_ABS'||��ˮ��)
				        ,RWAIE.ZCCBH                                                    AS ABSPOOLID           --֤ȯ���ʲ��ش���_�ʲ��ش���
				        ,RWAIP.ZQHFQRZZJGDM                                             AS ABSORIGINATORID     --֤ȯ�������˴���_֤ȯ����������֯��������
				        ,OI.SORTNO                                                      AS ORGSORTNO           --�������������
				        ,RWAIE.YWSSJG                                                   AS ORGID               --��������_��������
				        ,OI.ORGNAME                                                     AS ORGNAME             --������������
				        ,RWAIE.TX                                                       AS BUSINESSLINE        --����_����
				        ,'310'                                                          AS ASSETTYPE           --�ʲ�����_(Ĭ��Ϊ�ʲ�֤ȯ��)
				        ,'31001'                                                        AS ASSETSUBTYPE        --�ʲ�С��_(Ĭ��Ϊ�ʲ�֤ȯ��)
				        ,CASE WHEN RWAIE.YHJS IN ('01','02') THEN '020601'
				              WHEN RWAIE.YHJS='03' THEN '020602'
				              WHEN RWAIE.YHJS='04' THEN '020603'
				              WHEN RWAIE.YHJS='05' THEN '020604'
				              ELSE '020605'
				         END						                                                AS EXPOCATEGORYIRB     --���ձ�¶���_���н�ɫ("��ֵ��ExpoCategoryIRB")
				        ,CASE WHEN RWAIE.YHJS IN ('03','04','05')  OR RWAIE.TQTHLXDM IS NOT NULL THEN '02'
				              ELSE '01'
				         END												                                    AS EXPOBELONG          --��¶������ʶ          --��¶������ʶ_��¶������ʶ��  �������ʲ�֤ȯ������Ϊ01��02��03��04����¶��ʶΪ02-���⣬����Ϊ01-����
				        ,CASE WHEN FBI.ASSET_CLASS = '10' THEN '02'
				        			ELSE '01'
				         END												                                    AS BOOKTYPE            --�˻����_�˻����
				        ,'21907'                                                        AS ASSETTYPEOFHAIRCUTS --�ۿ�ϵ����Ӧ�ʲ�����_(Ĭ��Ϊ���� �ʲ����� AssetCategory �еġ�21907 ����)
				        ,RWAIE.ZZCZQHBZ                                                 AS REABSFLAG           --���ʲ�֤ȯ����ʶ
				        ,CASE WHEN RWAIP.JCZCYWLX IN ('02','03','04')	THEN '1'																 --02 ס����Ѻ����,03 ������Ѻ����,04 ���ÿ�
				              ELSE '0'
				         END											                                      AS RETAILFLAG          --���۱�ʶ
				        ,'02'                                                           AS REGUTRANTYPE        --��ܽ�������_(Ĭ��Ϊ02-�����ʱ��г����� ��ֵ��ReguTranType)
				        ,1                                                              AS REVAFREQUENCY       --�ع�Ƶ��_(Ĭ��Ϊ1��)
				        ,CASE WHEN RWAIE.TQTHLXDM IS NOT NULL THEN '03'
				        			WHEN RWAIE.YHJS = '03' THEN '04'
				              WHEN RWAIE.YHJS = '04' THEN '01'
				              WHEN RWAIE.YHJS = '05' THEN '02'
				              ELSE ''
				         END						                                                AS OFFABSBUSINESSTYPE  --�����ʲ�֤ȯ������
				        ,RWAIE.YHJS                                                     AS ABSROLE             --�ʲ�֤ȯ����ɫ_���н�ɫ
				        ,RWAIE.SFTGXYZCBFYDWBPJ                                         AS PROVIDECSRERFLAG    --�Ƿ��ṩ����֧�ֲ���ӳ���ⲿ����_�Ƿ��ṩ����֧�ֲ���ӳ���ⲿ����
				        ,RWAIE.XYFXHSSFTGGTBMDDJG                                       AS PROVIDECRMFSPEFLAG  --�Ƿ��ṩ���÷��ջ��͸��ر�Ŀ�Ļ���_���÷��ջ����Ƿ��ṩ���ر�Ŀ�Ļ���
				        ,CASE WHEN RWAIE.FQSHSTGZWBPJ IS NULL THEN '0124'
				         ELSE RWA_DEV.GETSTANDARDRATING1(RWAIE.FQSHSTGZWBPJ)
				         END                                                            AS MITIPROVMERATING    --�ṩ����ʱ�ⲿ����_����ʱ�����ṩ���ⲿ����
				        ,CASE WHEN RWAIE.DQHSTGZWBPJ IS NULL THEN '0124'
				         ELSE RWA_DEV.GETSTANDARDRATING1(RWAIE.DQHSTGZWBPJ)
				         END                                                            AS MITIPROVCERATING    --��ǰ�ⲿ����_��ǰ�����ṩ���ⲿ����
				        ,CASE WHEN RWAIE.HGLDXBLBZ='1' OR RWAIE.HGXJTZBLBZ='1' THEN '1'
				              ELSE '0'
				         END                                              							AS QUALFACIFLAG        --�ϸ������ʶ_"�ϸ������Ա�����ʶ�ϸ��ֽ�͸֧������ʶ"("���ϸ������Ա�����ʶ��ϸ��ֽ�͸֧������ʶ������һ��Ϊ'1-��'������Ϊ�ǣ�������Ϊ�� 1 �� 0 ��")
				        ,RWAIE.XJTZBLSFKSSWTJCX                                         AS UNCONDCANCELFLAG    --�Ƿ����ʱ����������_�ֽ�͸֧�����Ƿ����ʱ����������
				        ,RWAIE.FDSXH                                                    AS TRENCHSN            --�ֵ�˳���_�ֵ�˳���
				        ,RWAIE.DCMC                                                     AS TRENCHNAME          --��������_��������
				        ,RWAIE.SFZYXDC                                                  AS TOPTRENCHFLAG       --�Ƿ���ߵ���_�Ƿ������ȵ���
				        ,RWAIE.SFZYXDC                                                  AS PREFEREDTRENCHFLAG  --�Ƿ������ȵ���_�Ƿ������ȵ���
				        ,CASE WHEN RWAIE.ZQWBPJDJ IS NULL THEN '01'
				         ELSE NVL(RWAIE.ZQWBPJQX,SUBSTR(RWA_DEV.GETSTANDARDRATING1(RWAIE.ZQWBPJDJ),1,2))
				         END                                                            AS RATINGDURATIONTYPE  --������������_�ⲿ�������
				        ,CASE WHEN RWAIE.ZQWBPJDJ IS NULL THEN '0124'
				         ELSE RWA_DEV.GETSTANDARDRATING1(RWAIE.ZQWBPJDJ)
				         END                                                            AS ERATINGRESULT       --�ⲿ�������_�ⲿ�������
				        ,'0124'                                                         AS INFERRATINGRESULT   --�Ʋ��������_(Ĭ��Ϊδ���� 0124 δ����)
				        ,TO_CHAR(TO_DATE(RWAIE.FXR,'YYYY-MM-DD'),'YYYYMMDD')            AS ISSUEDATE           --��������_������
				        ,TO_CHAR(TO_DATE(RWAIE.DQR,'YYYY-MM-DD'),'YYYYMMDD')            AS DUEDATE             --��������_������
				        ,CASE WHEN (TO_DATE(RWAIE.DQR,'YYYY-MM-DD')-TO_DATE(RWAIE.FXR,'YYYY-MM-DD')) / 365 < 0
				              THEN 0
				              ELSE (TO_DATE(RWAIE.DQR,'YYYY-MM-DD')-TO_DATE(RWAIE.FXR,'YYYY-MM-DD')) / 365
				        END                                                             AS ORIGINALMATURITY    --ԭʼ����_"�����շ�����"("������-����������Ϊ��λ")
				        ,CASE WHEN (TO_DATE(RWAIE.DQR,'YYYY-MM-DD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD')) / 365 < 0
				              THEN 0
				              ELSE (TO_DATE(RWAIE.DQR,'YYYY-MM-DD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD')) / 365
				        END                                                             AS RESIDUALM           --ʣ������_"�����շ�����"("������-��ǰ������Ϊ��λ")
				        ,ROUND(TO_NUMBER(REPLACE(NVL(RWAIE.ZCYE,'0'),',','')),6)
				                                                                        AS ASSETBALANCE        --�ʲ����_�ʲ����
				        ,NVL(RWAIE.BZ,'CNY')                                            AS CURRENCY            --����_����
				        ,ROUND(TO_NUMBER(REPLACE(NVL(RWAIE.JZZB,'0'),',','')),6)
				                                                                        AS PROVISIONS          --��ֵ׼��_��ֵ׼��
				        ,0                                                              AS L                   --������������ˮƽ(Ĭ�� 0)
				        ,0                                                              AS T                   --���κ��(Ĭ�� 0)
				        ,RWAIE.TQTHLXDM                                                 AS EARLYAMORTTYPE      --��ǰ̯������(Ĭ�� ��)
				        ,RWAIE.LSCNLXDM                                                 AS RETAILCOMMITTYPE    --���۳�ŵ����(Ĭ�� ��)
				        ,NVL(TO_NUMBER(REPLACE(RWAIE.SGYPJCELC,',','')),0)              AS AVERIGONTHREEMTHS   --������ƽ����������(Ĭ�� 0)
				        ,NVL(TO_NUMBER(REPLACE(RWAIE.CELCSDD,',','')),0)	              AS INTGAPSTOPPOINT     --��������������(Ĭ�� 0)
				        ,CASE WHEN NVL(TO_NUMBER(REPLACE(RWAIE.CELCSDD,',','')),0) <> 0 THEN NVL(TO_NUMBER(REPLACE(RWAIE.SGYPJCELC,',','')),0) / NVL(TO_NUMBER(REPLACE(RWAIE.CELCSDD,',','')),0)
				         ELSE 0
				         END                                                            AS R                   --R_(Ĭ�� 0)
				        ,RWAIE.FXBLZB                                                   AS ISSUSERASSETPROP    --���л��������ʲ�ռ��
				        ,NULL                                                           AS INVESTOR            --Ͷ����Ȩ��(Ĭ�� 0)

    FROM 				RWA.RWA_WS_ABS_ISSUE_EXPOSURE RWAIE
    INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT RWD
    ON          RWAIE.SUPPORGID = RWD.ORGID
    AND         RWD.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND         RWD.SUPPTMPLID = 'M-0131'
    AND         RWD.SUBMITFLAG = '1'
    INNER JOIN  (SELECT  T1.ZCCBH
                         ,T1.ZQHFQRZZJGDM
                         ,T1.JCZCYWLX
                  FROM RWA.RWA_WS_ABS_ISSUE_POOL T1
            INNER JOIN RWA.RWA_WP_DATASUPPLEMENT RWD
                    ON T1.SUPPORGID = RWD.ORGID
                   AND RWD.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
                   AND RWD.SUPPTMPLID = 'M-0132'
                   AND RWD.SUBMITFLAG = '1'
                 WHERE T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
                ) RWAIP                       												--�ʲ�֤ȯ��-���л���-��Լ��ز�¼��
    ON          RWAIP.ZCCBH = RWAIE.ZCCBH
    LEFT JOIN   RWA.ORG_INFO OI                                       --������
    ON          RWAIE.YWSSJG = OI.ORGID
    LEFT JOIN   RWA_DEV.FNS_BND_INFO_B FBI                            --ծȯ��Ϣ��
    ON          FBI.BOND_ID = RWAIE.ZQNM
    AND         FBI.DATANO = p_data_dt_str
    WHERE 			RWAIE.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND   			RWAIE.YHJS <> '02'																		--���� 02 Ͷ�ʻ���  ����ȫ�ŵ����л�����¶��
    ;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ABS_ISSURE_ABSEXPOSURE',cascade => true);
    --DBMS_OUTPUT.PUT_LINE('���������롾�ʲ�֤ȯ�����л������ձ�¶��' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_ABS_ISSURE_ABSEXPOSURE;
    --DBMS_OUTPUT.PUT_LINE('RWA_DEV.RWA_ABS_ISSURE_ABSEXPOSURE-�ʲ�֤ȯ�����л������ձ�¶���в�������Ϊ��' || v_count1 || '��');

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count1;

    commit;
    --�����쳣
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '���롾�ʲ�֤ȯ�����л������ձ�¶��('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_ABS_ISSURE_ABSEXPOSURE;
/

