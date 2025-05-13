CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_ABSPOOL(
			 											p_data_dt_str	IN	VARCHAR2,		--��������
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_EI_ABSPOOL
    ʵ�ֹ���:��Լ�����Ϣ���ܱ�,�����Լ�����Ϣ
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�
    ��  ��  :V1.0.0
    ��д��  :SHUXD
    ��дʱ��:2016-07-11
    ��  λ	:�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1	:RWA_DEV.RWA_ABS_INVEST_ABSPOOL|�ʲ�֤ȯ��-Ͷ�ʻ�����Լ�����Ϣ��
    Դ  ��2	:RWA_DEV.RWA_ABS_ISSURE_ABSPOOL|�ʲ�֤ȯ��-���л�����Լ�����Ϣ��
    Ŀ���  :RWA_DEV.RWA_EI_ABSPOOL|��Լ�����Ϣ���ܱ�
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����)��
  	*/
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_ABSPOOL';
  --�����ж�ֵ����
  v_count INTEGER;
  --�����쳣����
  v_raise EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    BEGIN
    --ɾ�������ڵķ���������˴洢�����ǵ�һ�����׳��쳣�����Ժ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_ABSPOOL DROP PARTITION ABSPOOL' || p_data_dt_str;

    COMMIT;

    EXCEPTION
      WHEN OTHERS THEN
        IF (SQLCODE <> '-2149') THEN
          --�״η���truncate�����2149�쳣
         p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '���ܺ�Լ�����Ϣ��('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
        END IF;
    END;

    --����һ����ǰ�����µķ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_ABSPOOL ADD PARTITION ABSPOOL' || p_data_dt_str || ' VALUES(TO_DATE('|| p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;

    /*�����ʲ�֤ȯ��-Ͷ�ʻ����ĺ�Լ�����Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_ABSPOOL(
               DataDate         	                    --��������
              ,DataNo                                 --������ˮ��
              ,ABSPoolID                              --֤ȯ���ʲ���ID
              ,ABSOriginatorID                        --֤ȯ��������ID
              ,OrgID                                  --��������ID
              ,ABSName                                --�ʲ�֤ȯ������
              ,IRBBankFlag                            --��������Ƿ���������
              ,ABSType                                --�ʲ�֤ȯ������
              ,UnderAssetType                         --�����ʲ�����
              ,OriginatorFlag   	                    --�Ƿ������
              ,SatisfyManageFlag	                    --�Ƿ���Ϲ�������
              ,ComplianceABSFlag                      --�Ƿ�Ϲ��ʲ�֤ȯ��
              ,ProvideISFlag                          --�Ƿ��ṩ����֧��
              ,SaleGains                              --��������
              ,PropUnderAssetIRB                      --�����ʲ������������������
              ,SimplAlgoFlag                          --�Ƿ���ü򻯷���
              ,LargestExpoPP                          --�����ձ�¶���ʲ���Ϸݶ�
              ,ORGSORTNO                              --�������������
              ,ORGNAME                                --��������
              ,BUSINESSLINE                           --����
              ,ASSETSUBTYPE                           --�ʲ�С��
              ,ASSETTYPE                              --�ʲ�����
              ,REABSFLAG                              --���ʲ�֤ȯ����ʶ
)
     SELECT
                 DataDate                                             AS DataDate           --��������
                ,DataNo                                               AS DataNo             --������ˮ��
                ,ABSPoolID                                            AS ABSPoolID          --֤ȯ���ʲ���ID
                ,ABSOriginatorID                                      AS ABSOriginatorID    --֤ȯ��������ID
                ,OrgID                                                AS OrgID              --��������ID
                ,ABSName                                              AS ABSName            --�ʲ�֤ȯ������
                ,IRBBankFlag                                          AS IRBBankFlag        --��������Ƿ���������
                ,ABSType                                              AS ABSType            --�ʲ�֤ȯ������
                ,UnderAssetType                                       AS UnderAssetType     --�����ʲ�����
                ,OriginatorFlag                                       AS OriginatorFlag     --�Ƿ������
                ,SatisfyManageFlag                                    AS SatisfyManageFlag  --�Ƿ���Ϲ�������
                ,ComplianceABSFlag                                    AS ComplianceABSFlag  --�Ƿ�Ϲ��ʲ�֤ȯ��
                ,ProvideISFlag                                        AS ProvideISFlag      --�Ƿ��ṩ����֧��
                ,SaleGains                                            AS SaleGains          --��������
                ,PropUnderAssetIRB                                    AS PropUnderAssetIRB  --�����ʲ������������������
                ,SimplAlgoFlag                                        AS SimplAlgoFlag      --�Ƿ���ü򻯷���
                ,LargestExpoPP                                        AS LargestExpoPP      --�����ձ�¶���ʲ���Ϸݶ�
                ,ORGSORTNO                                            AS ORGSORTNO          --�������������
                ,ORGNAME                                              AS ORGNAME            --��������
                ,BUSINESSLINE                                         AS BUSINESSLINE       --����
                ,ASSETSUBTYPE                                         AS ASSETSUBTYPE       --�ʲ�С��
                ,ASSETTYPE                                            AS ASSETTYPE          --�ʲ�����
                ,REABSFLAG                                            AS REABSFLAG          --���ʲ�֤ȯ����ʶ
    FROM 				RWA_DEV.RWA_ABS_INVEST_ABSPOOL
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*�����ʲ�֤ȯ��-���л����ĺ�Լ�����Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_ABSPOOL(
               DataDate         	                    --��������
              ,DataNo                                 --������ˮ��
              ,ABSPoolID                              --֤ȯ���ʲ���ID
              ,ABSOriginatorID                        --֤ȯ��������ID
              ,OrgID                                  --��������ID
              ,ABSName                                --�ʲ�֤ȯ������
              ,IRBBankFlag                            --��������Ƿ���������
              ,ABSType                                --�ʲ�֤ȯ������
              ,UnderAssetType                         --�����ʲ�����
              ,OriginatorFlag   	                    --�Ƿ������
              ,SatisfyManageFlag	                    --�Ƿ���Ϲ�������
              ,ComplianceABSFlag                      --�Ƿ�Ϲ��ʲ�֤ȯ��
              ,ProvideISFlag                          --�Ƿ��ṩ����֧��
              ,SaleGains                              --��������
              ,PropUnderAssetIRB                      --�����ʲ������������������
              ,SimplAlgoFlag                          --�Ƿ���ü򻯷���
              ,LargestExpoPP                          --�����ձ�¶���ʲ���Ϸݶ�
              ,ORGSORTNO                              --�������������
              ,ORGNAME                                --��������
              ,BUSINESSLINE                           --����
              ,ASSETSUBTYPE                           --�ʲ�С��
              ,ASSETTYPE                              --�ʲ�����
              ,REABSFLAG                              --���ʲ�֤ȯ����ʶ
)
     SELECT
                 DataDate                                             AS DataDate           --��������
                ,DataNo                                               AS DataNo             --������ˮ��
                ,ABSPoolID                                            AS ABSPoolID          --֤ȯ���ʲ���ID
                ,ABSOriginatorID                                      AS ABSOriginatorID    --֤ȯ��������ID
                ,OrgID                                                AS OrgID              --��������ID
                ,ABSName                                              AS ABSName            --�ʲ�֤ȯ������
                ,IRBBankFlag                                          AS IRBBankFlag        --��������Ƿ���������
                ,ABSType                                              AS ABSType            --�ʲ�֤ȯ������
                ,UnderAssetType                                       AS UnderAssetType     --�����ʲ�����
                ,OriginatorFlag                                       AS OriginatorFlag     --�Ƿ������
                ,SatisfyManageFlag                                    AS SatisfyManageFlag  --�Ƿ���Ϲ�������
                ,ComplianceABSFlag                                    AS ComplianceABSFlag  --�Ƿ�Ϲ��ʲ�֤ȯ��
                ,ProvideISFlag                                        AS ProvideISFlag      --�Ƿ��ṩ����֧��
                ,SaleGains                                            AS SaleGains          --��������
                ,PropUnderAssetIRB                                    AS PropUnderAssetIRB  --�����ʲ������������������
                ,SimplAlgoFlag                                        AS SimplAlgoFlag      --�Ƿ���ü򻯷���
                ,LargestExpoPP                                        AS LargestExpoPP      --�����ձ�¶���ʲ���Ϸݶ�
                ,ORGSORTNO                                            AS ORGSORTNO          --�������������
                ,ORGNAME                                              AS ORGNAME            --��������
                ,BUSINESSLINE                                         AS BUSINESSLINE       --����
                ,ASSETSUBTYPE                                         AS ASSETSUBTYPE       --�ʲ�С��
                ,ASSETTYPE                                            AS ASSETTYPE          --�ʲ�����
                ,REABSFLAG                                            AS REABSFLAG          --���ʲ�֤ȯ����ʶ
    FROM 				RWA_DEV.RWA_ABS_ISSURE_ABSPOOL
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


INSERT INTO
 RWA_EI_ABSPOOL
(
DATADATE         --��������
,DATANO         --������ˮ��
,ABSPOOLID         --֤ȯ���ʲ���ID
,ABSORIGINATORID         --֤ȯ��������ID
,ORGSORTNO         --������ˮ��
,ORGID         --��������ID
,ORGNAME         --��������
,BUSINESSLINE         --����
,ASSETTYPE         --�ʲ�����
,ASSETSUBTYPE         --�ʲ�С��
,ABSNAME         --�ʲ�֤ȯ������
,IRBBANKFLAG         --��������Ƿ���������
,ABSTYPE         --�ʲ�֤ȯ������
,UNDERASSETTYPE         --�����ʲ�����
,ORIGINATORFLAG         --�Ƿ������
,SATISFYMANAGEFLAG         --�Ƿ���Ϲ�������
,COMPLIANCEABSFLAG         --�Ƿ�Ϲ��ʲ�֤ȯ��
,PROVIDEISFLAG         --�Ƿ��ṩ����֧��
,SALEGAINS         --��������
,PROPUNDERASSETIRB         --�����ʲ������������������
,SIMPLALGOFLAG         --�Ƿ���ü򻯷���
,LARGESTEXPOPP         --�����ձ�¶���ʲ���Ϸݶ�
,REABSFLAG         --���ʲ�֤ȯ����ʶ
,N         --���ձ�¶��Ч����
)
SELECT
DATADATE         --��������
,DATANO         --������ˮ��
,ABSPOOLID         --֤ȯ���ʲ���ID
,ABSORIGINATORID         --֤ȯ��������ID
,ORGSORTNO         --������ˮ��
,ORGID         --��������ID
,ORGNAME         --������������
,BUSINESSLINE         --����
,ASSETTYPE         --�ʲ�����
,ASSETSUBTYPE         --�ʲ�С��
,''         --�ʲ�֤ȯ������
,''         --��������Ƿ���������
,'01'         --�ʲ�֤ȯ������
,'01'         --�����ʲ�����
,''         --�Ƿ������
,''         --�Ƿ���Ϲ�������
,''         --�Ƿ�Ϲ��ʲ�֤ȯ��
,''         --�Ƿ��ṩ����֧��
,''         --��������
,''         --�����ʲ������������������
,''         --�Ƿ���ü򻯷���
,''         --�����ձ�¶���ʲ���Ϸݶ�
,''         --���ʲ�֤ȯ����ʶ
,''       --���ձ�¶��Ч����
FROM RWA_EI_ABSEXPOSURE T WHERE T.DATADATE= TO_DATE(p_data_dt_str, 'YYYYMMDD');
COMMIT;
/*by chengang*/


    --�������Ϣ
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_ABSPOOL',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_ABSPOOL',partname => 'ABSPOOL'||p_data_dt_str,granularity => 'PARTITION',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_ABSPOOL WHERE DATANO = p_data_dt_str;
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_ABSPOOL��ǰ��������ݼ�¼Ϊ:' || v_count1 || '��');

    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '�ʲ�֤ȯ���ĺ�Լ�����Ϣ��(RWA_DEV.PRO_RWA_EI_ABSPOOL)ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_EI_ABSPOOL;
/

