CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_ABSEXPOSURE(
                                                p_data_dt_str IN VARCHAR2,    --��������
                                                p_po_rtncode OUT VARCHAR2,    --���ر�� 1 �ɹ�,0 ʧ��
                                                p_po_rtnmsg OUT  VARCHAR2    --��������
)
  /*
    �洢��������:PRO_RWA_EI_ABSEXPOSURE
    ʵ�ֹ���:���ձ�¶���ܱ�,������ձ�¶��Ϣ
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�
    ��  ��  :V1.0.0
    ��д��  :SHUXD
    ��дʱ��:2016-07-11
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.RWA_ABS_INVEST_ABSEXPOSURE|�ʲ�֤ȯ��-Ͷ�ʻ������ձ�¶��
    Դ  ��2 :RWA_DEV.RWA_ABS_ISSURE_ABSEXPOSURE|�ʲ�֤ȯ��-���л������ձ�¶��
    Ŀ���  :RWA_DEV.RWA_EI_ABSEXPOSURE|���ձ�¶���ܱ�
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����)��
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_ABSEXPOSURE';
  --�����ж�ֵ����
  v_count INTEGER;
  --�����쳣����
  v_raise EXCEPTION;
  --������Ʒ���
  v_ye varchar2(20);

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    BEGIN
    --ɾ�������ڵķ���������˴洢�����ǵ�һ�����׳��쳣�����Ժ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_ABSEXPOSURE DROP PARTITION ABSEXPOSURE' || p_data_dt_str;

    COMMIT;


/*����¼������Ʒ����� */

delete from rwa.rwa_ws_abs_bl
t1 where t1.datadate=TO_DATE(p_data_dt_str, 'YYYYMMDD')
and t1.suppserialno IN (select  t2.suppserialno
    from rwa.rwa_ws_abs7_bl t2
   where t2.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'));
   
   commit;

insert into rwa.rwa_ws_abs_bl
  select *
    from rwa.rwa_ws_abs7_bl t
   where t.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD');

commit;

select count(*)
  into v_count
  from rwa.rwa_ws_abs_bl
 where dklx = '���˴���'
   and DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD');
IF V_COUNT = 1 THEN
  select ye / 2
    into v_ye
    from rwa.RWA_WS_ABS_BL
   where dklx = '���˴���'
     and DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD');
      ELSE v_ye:=0;
END IF;

/*update rwa.RWA_WS_ABS_BL b
   set b.ye =
       (b.ye + v_ye)
 where b.dklx = '����ס�����Ҵ���'
   and DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD');

update rwa.RWA_WS_ABS_BL b
   set b.ye =
       (b.ye + v_ye)
 where b.dklx = '��˾����'
   and DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD');

update rwa.RWA_WS_ABS_BL b
   set b.ye = '0'
 where b.dklx = '���˴���'
   and DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD');

commit;*/

    EXCEPTION
      WHEN OTHERS THEN
        IF (SQLCODE <> '-2149') THEN
          --�״η���truncate�����2149�쳣
         p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '�����ʲ�֤ȯ����¶��Ϣ��('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
        END IF;
    END;

    --����һ����ǰ�����µķ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_ABSEXPOSURE ADD PARTITION ABSEXPOSURE' || p_data_dt_str || ' VALUES(TO_DATE('|| p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;

    /*�����ʲ�֤ȯ��-Ͷ�ʻ����ķ��ձ�¶��Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_ABSEXPOSURE(
               DataDate                                 --��������
              ,DataNo                                   --������ˮ��
              ,ABSExposureID                            --�ʲ�֤ȯ�����ձ�¶ID
              ,ABSPoolID                                --֤ȯ���ʲ���ID
              ,ABSOriginatorID                          --֤ȯ��������ID
              ,OrgID                                    --��������ID
              ,OrgName                                  --������������
              ,Businessline                             --����
              ,AssetType                                --�ʲ�����
              ,AssetSubType                             --�ʲ�С��
              ,ExpoCategoryIRB                          --��������¶���
              ,ExpoBelong                               --��¶������ʶ
              ,BookType                                 --�˻����
              ,AssetTypeOfHaircuts                      --�ۿ�ϵ����Ӧ�ʲ����
              ,ABSRole                                  --�ʲ�֤ȯ����ɫ
              ,ProvideCRMFSPEFlag                       --�Ƿ��ṩ���÷��ջ��͸��ر�Ŀ�Ļ���
              ,ProvideCSRERFlag                         --�Ƿ��ṩ����֧�ֲ���ӳ���ⲿ����
              ,ReABSFlag                                --���ʲ�֤ȯ����ʶ
              ,RetailFlag                               --���۱�ʶ
              ,QualFaciFlag                             --�ϸ������ʶ
              ,UncondCancelFlag                         --�Ƿ����ʱ����������
              ,TrenchSN                                 --�ֵ�˳���
              ,TrenchName                               --��������
              ,TopTrenchFlag                            --�Ƿ���ߵ���
              ,PreferedTrenchFlag                       --�Ƿ������ȵ���
              ,ReguTranType                             --��ܽ�������
              ,RevaFrequency                            --�ع�Ƶ��
              ,MitiProvMERating                         --�����ṩ�߻���ʱ�ⲿ����
              ,MitiProvCERating                         --�����ṩ�ߵ�ǰ�ⲿ����
              ,RatingDurationType                       --������������
              ,ERatingResult                            --�ⲿ�������
              ,InferRatingResult                        --�Ʋ��������
              ,IssueDate                                --��������
              ,DueDate                                  --��������
              ,OriginalMaturity                         --ԭʼ����
              ,ResidualM                                --ʣ������
              ,AssetBalance                             --�ʲ����
              ,Currency                                 --����
              ,Provisions                               --��ֵ׼��
              ,L                                        --������������ˮƽ
              ,T                                        --���κ��
              ,EarlyAmortType                           --��ǰ̯������
              ,RetailCommitType                         --���۳�ŵ����
              ,Investor                                 --Ͷ����Ȩ��
              ,AverIGOnThreeMths                        --������ƽ����������
              ,IntGapStopPoint                          --��������������
              ,R                                        --������ƽ����������/������
              ,OFFABSBUSINESSTYPE                       --�����ʲ�֤ȯ������
              ,ORGSORTNO                                --�������������
              ,ISSUSERASSETPROP                         --���л��������ʲ�ռ��
)
     SELECT
                 DataDate                                                AS DataDate             --��������
                ,DataNo                                                  AS DataNo               --������ˮ��
                ,ABSExposureID                                           AS ABSExposureID        --�ʲ�֤ȯ�����ձ�¶ID
                ,ABSPoolID                                               AS ABSPoolID            --֤ȯ���ʲ���ID
                ,ABSOriginatorID                                         AS ABSOriginatorID      --֤ȯ��������ID
                ,OrgID                                                   AS OrgID                --��������ID
                ,OrgName                                                 AS OrgName              --������������
                ,Businessline                                            AS Businessline         --����
                ,AssetType                                               AS AssetType            --�ʲ�����
                ,AssetSubType                                            AS AssetSubType         --�ʲ�С��
                ,ExpoCategoryIRB                                         AS ExpoCategoryIRB      --��������¶���
                ,ExpoBelong                                              AS ExpoBelong           --��¶������ʶ
                ,BookType                                                AS BookType             --�˻����                           ?
                ,AssetTypeOfHaircuts                                     AS AssetTypeOfHaircuts  --�ۿ�ϵ����Ӧ�ʲ����
                ,ABSRole                                                 AS ABSRole              --�ʲ�֤ȯ����ɫ
                ,ProvideCRMFSPEFlag                                      AS ProvideCRMFSPEFlag   --�Ƿ��ṩ���÷��ջ��͸��ر�Ŀ�Ļ���
                ,ProvideCSRERFlag                                        AS ProvideCSRERFlag     --�Ƿ��ṩ����֧�ֲ���ӳ���ⲿ����
                ,ReABSFlag                                               AS ReABSFlag            --���ʲ�֤ȯ����ʶ
                ,RetailFlag                                              AS RetailFlag           --���۱�ʶ
                ,QualFaciFlag                                            AS QualFaciFlag         --�ϸ������ʶ
                ,UncondCancelFlag                                        AS UncondCancelFlag     --�Ƿ����ʱ����������
                ,TrenchSN                                                AS TrenchSN             --�ֵ�˳���
                ,TrenchName                                              AS TrenchName           --��������
                ,TopTrenchFlag                                           AS TopTrenchFlag        --�Ƿ���ߵ���
                ,PreferedTrenchFlag                                      AS PreferedTrenchFlag   --�Ƿ������ȵ���
                ,ReguTranType                                            AS ReguTranType         --��ܽ�������
                ,RevaFrequency                                           AS RevaFrequency        --�ع�Ƶ��
                ,MitiProvMERating                                        AS MitiProvMERating     --�����ṩ�߻���ʱ�ⲿ����
                ,MitiProvCERating                                        AS MitiProvCERating     --�����ṩ�ߵ�ǰ�ⲿ����
                ,RatingDurationType                                      AS RatingDurationType   --������������
                ,ERatingResult                                           AS ERatingResult        --�ⲿ�������
                ,InferRatingResult                                       AS InferRatingResult    --�Ʋ��������
                ,TO_CHAR(TO_DATE(IssueDate,'YYYY-MM-DD'),'YYYY-MM-DD')   AS IssueDate            --��������
                ,TO_CHAR(TO_DATE(DueDate,'YYYY-MM-DD'),'YYYY-MM-DD')     AS DueDate              --��������
                ,OriginalMaturity                                        AS OriginalMaturity     --ԭʼ����
                ,ResidualM                                               AS ResidualM            --ʣ������
                ,AssetBalance                                            AS AssetBalance         --�ʲ����
                ,Currency                                                AS Currency             --����
                ,Provisions                                              AS Provisions           --��ֵ׼��
                ,L                                                       AS L                    --������������ˮƽ
                ,T                                                       AS T                    --���κ��
                ,EarlyAmortType                                          AS EarlyAmortType       --��ǰ̯������
                ,RetailCommitType                                        AS RetailCommitType     --���۳�ŵ����
                ,Investor                                                AS Investor             --Ͷ����Ȩ��
                ,AverIGOnThreeMths                                       AS AverIGOnThreeMths    --������ƽ����������
                ,IntGapStopPoint                                         AS IntGapStopPoint      --��������������
                ,R                                                       AS R                    --������ƽ����������/������
                ,OFFABSBUSINESSTYPE                                      AS OFFABSBUSINESSTYPE   --�����ʲ�֤ȯ������
                ,ORGSORTNO                                               AS ORGSORTNO            --�������������
                ,null                                                    AS ISSUSERASSETPROP     --���л��������ʲ�ռ��
    FROM 				RWA_DEV.RWA_ABS_INVEST_ABSEXPOSURE
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*�����ʲ�֤ȯ��-���л����ķ��ձ�¶��Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_ABSEXPOSURE(
               DataDate                                 --��������
              ,DataNo                                   --������ˮ��
              ,ABSExposureID                            --�ʲ�֤ȯ�����ձ�¶ID
              ,ABSPoolID                                --֤ȯ���ʲ���ID
              ,ABSOriginatorID                          --֤ȯ��������ID
              ,OrgID                                    --��������ID
              ,OrgName                                  --������������
              ,Businessline                             --����
              ,AssetType                                --�ʲ�����
              ,AssetSubType                             --�ʲ�С��
              ,ExpoCategoryIRB                          --��������¶���
              ,ExpoBelong                               --��¶������ʶ
              ,BookType                                 --�˻����
              ,AssetTypeOfHaircuts                      --�ۿ�ϵ����Ӧ�ʲ����
              ,ABSRole                                  --�ʲ�֤ȯ����ɫ
              ,ProvideCRMFSPEFlag                       --�Ƿ��ṩ���÷��ջ��͸��ر�Ŀ�Ļ���
              ,ProvideCSRERFlag                         --�Ƿ��ṩ����֧�ֲ���ӳ���ⲿ����
              ,ReABSFlag                                --���ʲ�֤ȯ����ʶ
              ,RetailFlag                               --���۱�ʶ
              ,QualFaciFlag                             --�ϸ������ʶ
              ,UncondCancelFlag                         --�Ƿ����ʱ����������
              ,TrenchSN                                 --�ֵ�˳���
              ,TrenchName                               --��������
              ,TopTrenchFlag                            --�Ƿ���ߵ���
              ,PreferedTrenchFlag                       --�Ƿ������ȵ���
              ,ReguTranType                             --��ܽ�������
              ,RevaFrequency                            --�ع�Ƶ��
              ,MitiProvMERating                         --�����ṩ�߻���ʱ�ⲿ����
              ,MitiProvCERating                         --�����ṩ�ߵ�ǰ�ⲿ����
              ,RatingDurationType                       --������������
              ,ERatingResult                            --�ⲿ�������
              ,InferRatingResult                        --�Ʋ��������
              ,IssueDate                                --��������
              ,DueDate                                  --��������
              ,OriginalMaturity                         --ԭʼ����
              ,ResidualM                                --ʣ������
              ,AssetBalance                             --�ʲ����
              ,Currency                                 --����
              ,Provisions                               --��ֵ׼��
              ,L                                        --������������ˮƽ
              ,T                                        --���κ��
              ,EarlyAmortType                           --��ǰ̯������
              ,RetailCommitType                         --���۳�ŵ����
              ,Investor                                 --Ͷ����Ȩ��
              ,AverIGOnThreeMths                        --������ƽ����������
              ,IntGapStopPoint                          --��������������
              ,R                                        --������ƽ����������/������
              ,OFFABSBUSINESSTYPE                       --�����ʲ�֤ȯ������
              ,ORGSORTNO                                --�������������
              ,ISSUSERASSETPROP                         --���л��������ʲ�ռ��
)
     SELECT
                 DataDate                                                AS DataDate             --��������
                ,DataNo                                                  AS DataNo               --������ˮ��
                ,ABSExposureID                                           AS ABSExposureID        --�ʲ�֤ȯ�����ձ�¶ID
                ,ABSPoolID                                               AS ABSPoolID            --֤ȯ���ʲ���ID
                ,ABSOriginatorID                                         AS ABSOriginatorID      --֤ȯ��������ID
                ,OrgID                                                   AS OrgID                --��������ID
                ,OrgName                                                 AS OrgName              --������������
                ,Businessline                                            AS Businessline         --����
                ,AssetType                                               AS AssetType            --�ʲ�����
                ,AssetSubType                                            AS AssetSubType         --�ʲ�С��
                ,ExpoCategoryIRB                                         AS ExpoCategoryIRB      --��������¶���
                ,ExpoBelong                                              AS ExpoBelong           --��¶������ʶ
                ,BookType                                                AS BookType             --�˻����                           ?
                ,AssetTypeOfHaircuts                                     AS AssetTypeOfHaircuts  --�ۿ�ϵ����Ӧ�ʲ����
                ,ABSRole                                                 AS ABSRole              --�ʲ�֤ȯ����ɫ
                ,ProvideCRMFSPEFlag                                      AS ProvideCRMFSPEFlag   --�Ƿ��ṩ���÷��ջ��͸��ر�Ŀ�Ļ���
                ,ProvideCSRERFlag                                        AS ProvideCSRERFlag     --�Ƿ��ṩ����֧�ֲ���ӳ���ⲿ����
                ,ReABSFlag                                               AS ReABSFlag            --���ʲ�֤ȯ����ʶ
                ,RetailFlag                                              AS RetailFlag           --���۱�ʶ
                ,QualFaciFlag                                            AS QualFaciFlag         --�ϸ������ʶ
                ,UncondCancelFlag                                        AS UncondCancelFlag     --�Ƿ����ʱ����������
                ,TrenchSN                                                AS TrenchSN             --�ֵ�˳���
                ,TrenchName                                              AS TrenchName           --��������
                ,TopTrenchFlag                                           AS TopTrenchFlag        --�Ƿ���ߵ���
                ,PreferedTrenchFlag                                      AS PreferedTrenchFlag   --�Ƿ������ȵ���
                ,ReguTranType                                            AS ReguTranType         --��ܽ�������
                ,RevaFrequency                                           AS RevaFrequency        --�ع�Ƶ��
                ,MitiProvMERating                                        AS MitiProvMERating     --�����ṩ�߻���ʱ�ⲿ����
                ,MitiProvCERating                                        AS MitiProvCERating     --�����ṩ�ߵ�ǰ�ⲿ����
                ,RatingDurationType                                      AS RatingDurationType   --������������
                ,ERatingResult                                           AS ERatingResult        --�ⲿ�������
                ,InferRatingResult                                       AS InferRatingResult    --�Ʋ��������
                ,TO_CHAR(TO_DATE(IssueDate,'YYYY-MM-DD'),'YYYY-MM-DD')   AS IssueDate            --��������
                ,TO_CHAR(TO_DATE(DueDate,'YYYY-MM-DD'),'YYYY-MM-DD')     AS DueDate              --��������
                ,OriginalMaturity                                        AS OriginalMaturity     --ԭʼ����
                ,ResidualM                                               AS ResidualM            --ʣ������
                ,AssetBalance                                            AS AssetBalance         --�ʲ����
                ,Currency                                                AS Currency             --����
                ,Provisions                                              AS Provisions           --��ֵ׼��
                ,L                                                       AS L                    --������������ˮƽ
                ,T                                                       AS T                    --���κ��
                ,EarlyAmortType                                          AS EarlyAmortType       --��ǰ̯������
                ,RetailCommitType                                        AS RetailCommitType     --���۳�ŵ����
                ,Investor                                                AS Investor             --Ͷ����Ȩ��
                ,AverIGOnThreeMths                                       AS AverIGOnThreeMths    --������ƽ����������
                ,IntGapStopPoint                                         AS IntGapStopPoint      --��������������
                ,R                                                       AS R                    --������ƽ����������/������
                ,OFFABSBUSINESSTYPE                                      AS OFFABSBUSINESSTYPE   --�����ʲ�֤ȯ������
                ,ORGSORTNO                                               AS ORGSORTNO            --�������������
                ,ISSUSERASSETPROP                                        AS ISSUSERASSETPROP     --���л��������ʲ�ռ��
    FROM 				RWA_DEV.RWA_ABS_ISSURE_ABSEXPOSURE
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;



--������Ʒ��¼

INSERT INTO 
RWA_EI_ABSEXPOSURE -- �ʲ�֤ȯ�����ձ�¶��
(DATADATE         --��������
,DATANO         --������ˮ��
,ABSEXPOSUREID         --�ʲ�֤ȯ�����ձ�¶ID
,ABSPOOLID         --֤ȯ���ʲ���ID
,ABSORIGINATORID         --֤ȯ��������ID
,ORGSORTNO         --������ˮ��
,ORGID         --��������ID
,ORGNAME         --������������
,BUSINESSLINE         --����
,ASSETTYPE         --�ʲ�����
,ASSETSUBTYPE         --�ʲ�С��
,EXPOCATEGORYIRB         --��������¶���
,EXPOBELONG         --��¶������ʶ
,BOOKTYPE         --�˻����
,ASSETTYPEOFHAIRCUTS         --�ۿ�ϵ����Ӧ�ʲ����
,ABSROLE         --�ʲ�֤ȯ����ɫ
,PROVIDECRMFSPEFLAG         --�Ƿ��ṩ���÷��ջ��͸��ر�Ŀ�Ļ���
,PROVIDECSRERFLAG         --�Ƿ��ṩ����֧�ֲ���ӳ���ⲿ����
,REABSFLAG         --���ʲ�֤ȯ����ʶ
,RETAILFLAG         --���۱�ʶ
,QUALFACIFLAG         --�ϸ������ʶ
,UNCONDCANCELFLAG         --�Ƿ����ʱ����������
,TRENCHSN         --�ֵ�˳���
,TRENCHNAME         --��������
,TOPTRENCHFLAG         --�Ƿ���ߵ���
,PREFEREDTRENCHFLAG         --�Ƿ������ȵ���
,REGUTRANTYPE         --��ܽ�������
,REVAFREQUENCY         --�ع�Ƶ��
,MITIPROVMERATING         --�����ṩ�߻���ʱ�ⲿ����
,MITIPROVCERATING         --�����ṩ�ߵ�ǰ�ⲿ����
,RATINGDURATIONTYPE         --������������
,ERATINGRESULT         --�ⲿ�������
,INFERRATINGRESULT         --�Ʋ��������
,ISSUEDATE         --��������
,DUEDATE         --��������
,ORIGINALMATURITY         --ԭʼ����
,RESIDUALM         --ʣ������
,ASSETBALANCE         --�ʲ����
,CURRENCY         --����
,PROVISIONS         --��ֵ׼��
,L         --������������ˮƽ
,T         --���κ��
,EARLYAMORTTYPE         --��ǰ̯������
,RETAILCOMMITTYPE         --���۳�ŵ����
,INVESTOR         --Ͷ����Ȩ��
,AVERIGONTHREEMTHS         --������ƽ����������
,INTGAPSTOPPOINT         --��������������
,R         --������ƽ����������/������
,OFFABSBUSINESSTYPE         --�����ʲ�֤ȯ��ҵ������
,ISSUSERASSETPROP         --���л��������ʲ�ռ��
)
SELECT 
  TO_DATE(p_data_dt_str,'YYYYMMDD')   AS DATADATE  -- ��������
 ,p_data_dt_str                  AS DATANO    ---������ˮ��
 ,CASE WHEN T1.DKLX='����ס�����Ҵ���' THEN 'B201712285095'
 WHEN  T1.DKLX='���˴���' THEN 'B201803296435A'
 WHEN  T1.DKLX='��˾����' THEN 'B201803296435B'
 END, --�ʲ�֤ȯ�����ձ�¶ID
 CASE WHEN T1.DKLX='����ס�����Ҵ���' THEN 'B201712285095'
 WHEN  T1.DKLX='���˴���' THEN 'B201803296435A'
 WHEN  T1.DKLX='��˾����' THEN 'B201803296435B'
 END, --֤ȯ���ʲ���ID
 '�������йɷ����޹�˾' --֤ȯ��������ID
 ,'1'         --������ˮ��
,'9998'         --��������ID
,'�������йɷ����޹�˾'         --������������
,'0501'         --����
,'310'         --�ʲ�����
,'31001'         --�ʲ�С��
,'020601'         --��������¶���
,'01'         --��¶������ʶ
,'01'         --�˻����
,'01'         --�ۿ�ϵ����Ӧ�ʲ����
,''         --�ʲ�֤ȯ����ɫ
,''         --�Ƿ��ṩ���÷��ջ��͸��ر�Ŀ�Ļ���
,''         --�Ƿ��ṩ����֧�ֲ���ӳ���ⲿ����
,'0'         --���ʲ�֤ȯ����ʶ
,'0'         --���۱�ʶ
,''         --�ϸ������ʶ
,''         --�Ƿ����ʱ����������
,''         --�ֵ�˳���
,''         --��������
,''         --�Ƿ���ߵ���
,''         --�Ƿ������ȵ���
,'02'         --��ܽ�������
,'01'         --�ع�Ƶ��
,''         --�����ṩ�߻���ʱ�ⲿ����
,''         --�����ṩ�ߵ�ǰ�ⲿ����
,''         --������������
,CASE WHEN T1.DKLX='����ס�����Ҵ���' THEN '0106' --50%
 WHEN  T1.DKLX='���˴���' THEN '0109' --100%
 WHEN  T1.DKLX='��˾����' THEN '0109' --100%
 END --�ʲ�֤ȯ�����ձ�¶ID         --�ⲿ�������
,''         --�Ʋ��������
,CASE WHEN T1.ZCZQHMC='����6��' THEN '2018-03-29'
 ELSE '2017-12-28' END     --��������
,CASE WHEN T1.ZCZQHMC='����6��' THEN '2027-09-28'
 ELSE '2021-03-29' END          --��������
,CASE WHEN T1.ZCZQHMC='����6��' THEN '9.756164384'
 ELSE '3.002739726' END         --ԭʼ����
,CASE WHEN T1.ZCZQHMC='����6��' THEN  ( CASE WHEN (TO_DATE(20270928,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                    THEN 0
                    ELSE (TO_DATE(20270928,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
              END )
 ELSE ( CASE WHEN (TO_DATE(20210329,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                    THEN 0
                    ELSE (TO_DATE(20210329,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
              END ) END   --ʣ������
, 

 CASE WHEN T1.DKLX='����ס�����Ҵ���' THEN T1.YE+v_ye
 WHEN  T1.DKLX='���˴���' THEN 0
 WHEN  T1.DKLX='��˾����' THEN T1.YE+v_ye
 END
 --�ʲ����
,'CNY'         --����
,'0'         --��ֵ׼��
,''         --������������ˮƽ
,''         --���κ��
,''         --��ǰ̯������
,''         --���۳�ŵ����
,''         --Ͷ����Ȩ��
,''         --������ƽ����������
,''         --��������������
,''         --������ƽ����������/������
,''         --�����ʲ�֤ȯ��ҵ������
,''         --���л��������ʲ�ռ��


FROM RWA.RWA_WS_ABS_BL T1
WHERE T1.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD');
COMMIT;

/*��ֵ*/

UPDATE RWA_EI_ABSEXPOSURE T
   SET T.PROVISIONS =
       (SELECT I9.FINAL_ECL
          FROM SYS_IFRS9_RESULT I9
         WHERE I9.CONTRACT_REFERENCE = T.ABSEXPOSUREID
           AND I9.DATANO = p_data_dt_str)
 WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
   and exists (SELECT 1
          FROM SYS_IFRS9_RESULT I9
         WHERE I9.CONTRACT_REFERENCE = T.ABSEXPOSUREID
           AND I9.DATANO = p_data_dt_str);
COMMIT;

    --�������Ϣ
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_ABSEXPOSURE',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_ABSEXPOSURE',partname => 'ABSEXPOSURE'||p_data_dt_str,granularity => 'PARTITION',cascade => true);


    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_ABSEXPOSURE WHERE DATANO = p_data_dt_str;
    --Dbms_output.Put_line('RWA_EI_ABSEXPOSURE��ǰ��������ݼ�¼Ϊ:' || v_count1 || '��');

    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '�ʲ�֤ȯ���ķ��ձ�¶��(RWA_DEV.PRO_RWA_EI_ABSEXPOSURE)ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_EI_ABSEXPOSURE;
/

