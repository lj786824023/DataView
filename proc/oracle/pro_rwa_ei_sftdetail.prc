CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_SFTDETAIL(
                             p_data_dt_str  IN  VARCHAR2,    --�������� yyyyMMdd
                             p_po_rtncode  OUT  VARCHAR2,    --���ر�� 1 �ɹ�,0 ʧ��
                            p_po_rtnmsg    OUT  VARCHAR2    --��������
        )
  /*
    �洢��������:RWA_DEV.PRO_RWA_EI_SFTDETAIL
    ʵ�ֹ���:����������ϻع���Ϣ
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :SHUXD
    ��дʱ��:2016-06-01
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.RWA_HG_SFTDETAIL|��ϻع���
    Ŀ���  :RWA_DEV.RWA_EI_SFTDETAIL|������ϻع���
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_SFTDETAIL';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    BEGIN
    --ɾ�������ڵķ���������˴洢�����ǵ�һ�����׳��쳣�����Ժ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_SFTDETAIL DROP PARTITION SFTDETAIL' || p_data_dt_str;

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
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_SFTDETAIL ADD PARTITION SFTDETAIL' || p_data_dt_str || ' VALUES(TO_DATE('|| p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;

    /*����ع������ϻع���Ϣ��Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_SFTDETAIL(
                DATADATE                               --��������
                ,DATANO                                  --������ˮ��
                ,SFTDETAILID                             --֤ȯ���ʽ�����ϸID
                ,MASTERNETAGREEID                        --���������Э��ID
                ,EXPOSUREID                              --���ձ�¶ID
                ,SECUID                                  --֤ȯID
                ,SSYSID                                  --ԴϵͳID
                ,BOOKTYPE                                --�˻����
                ,TRANROLE                                --���׽�ɫ
                ,TRADINGASSETTYPE                        --�����ʲ�����
                ,CLAIMSLEVEL                             --ծȨ����
                ,QUALFLAGSTD                             --Ȩ�ط��ϸ��ʶ
                ,QUALFLAGFIRB                            --�����������ϸ��ʶ
                ,COLLATERALSDVSSTD                      --Ȩ�ط�����ѺƷϸ��
                ,STARTDATE                               --��ʼ����
                ,DUEDATE                                 --��������
                ,ORIGINALMATURITY                        --ԭʼ����
                ,RESIDUALM                               --ʣ������
                ,ASSETBALANCE                            --�ʲ����
                ,ASSETCURRENCY                           --�ʲ�����
                ,APPZEROHAIRCUTSFLAG                     --�Ƿ��������ۿ�ϵ��
                ,INTEHAIRCUTSFLAG                        --���й����ۿ�ϵ����ʶ
                ,INTERNALHC                             --�ڲ��ۿ�ϵ��
                ,SECUISSUERID                            --֤ȯ������ID
                ,BONDISSUEINTENT                         --ծȯ����Ŀ��
                ,FCTYPE                                  --������ѺƷ����
                ,ABSFLAG                                 --�ʲ�֤ȯ����ʶ
                ,RATINGDURATIONTYPE                      --������������
                ,SECUISSUERATING                         --֤ȯ���еȼ�
                ,SECUREVAFREQUENCY                       --֤ȯ�ع�Ƶ��
                ,RCERating														 	 --�����˾���ע����ⲿ����
    )
    SELECT
                DATADATE                                AS DATADATE                 --��������
                ,DATANO                                 AS DATANO                   --������ˮ��
                ,SFTDETAILID                             AS SFTDETAILID               --֤ȯ���ʽ�����ϸID
                ,MASTERNETAGREEID                       AS MASTERNETAGREEID          --���������Э��ID
                ,EXPOSUREID                             AS EXPOSUREID                --���ձ�¶ID
                ,SECUID                                   AS SECUID                    --֤ȯID
                ,SSYSID                                  AS SSYSID                    --ԴϵͳID
                ,BOOKTYPE                                 AS BOOKTYPE                   --�˻����
                ,TRANROLE                                 AS TRANROLE                  --���׽�ɫ
                ,TRADINGASSETTYPE                        AS TRADINGASSETTYPE          --�����ʲ�����
                ,CLAIMSLEVEL                            AS CLAIMSLEVEL               --ծȨ����
                ,QUALFLAGSTD                            AS QUALFLAGSTD               --Ȩ�ط��ϸ��ʶ
                ,QUALFLAGFIRB                            AS QUALFLAGFIRB              --�����������ϸ��ʶ
                ,COLLATERALSDVSSTD                       AS COLLATERALSDVSSTD         --Ȩ�ط�����ѺƷϸ��
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                        AS STARTDATE                --��ʼ����
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                        AS DUEDATE                   --��������
                ,ORIGINALMATURITY                       AS ORIGINALMATURITY          --ԭʼ����
                ,RESIDUALM                              AS RESIDUALM                 --ʣ������
                ,ASSETBALANCE                            AS ASSETBALANCE              --�ʲ����
                ,ASSETCURRENCY                          AS ASSETCURRENCY             --�ʲ�����
                ,APPZEROHAIRCUTSFLAG                     AS APPZEROHAIRCUTSFLAG       --�Ƿ��������ۿ�ϵ��
                ,INTEHAIRCUTSFLAG                       AS INTEHAIRCUTSFLAG          --���й����ۿ�ϵ����ʶ
                ,INTERNALHC                             AS INTERNALHC                --�ڲ��ۿ�ϵ��
                ,SECUISSUERID                           AS SECUISSUERID             --֤ȯ������ID
                ,BONDISSUEINTENT                        AS BONDISSUEINTENT           --ծȯ����Ŀ��
                ,FCTYPE                                 AS FCTYPE                    --������ѺƷ����
                ,ABSFLAG                                AS ABSFLAG                   --�ʲ�֤ȯ����ʶ
                ,RATINGDURATIONTYPE                      AS RATINGDURATIONTYPE        --������������
                ,SECUISSUERATING                         AS SECUISSUERATING          --֤ȯ���еȼ�
                ,SECUREVAFREQUENCY                      AS SECUREVAFREQUENCY         --֤ȯ�ع�Ƶ��
                ,RCERating														 	AS RCERating								 --�����˾���ע����ⲿ����

    FROM   			RWA_DEV.RWA_HG_SFTDETAIL
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;

    --�������Ϣ
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_SFTDETAIL',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_SFTDETAIL',partname => 'SFTDETAIL'||p_data_dt_str,granularity => 'PARTITION',cascade => true);


    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_SFTDETAIL WHERE DATANO = p_data_dt_str;
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_SFTDETAIL��ǰ������ϻع���¼Ϊ: ' || v_count || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '��ϻع�('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_EI_SFTDETAIL;
/

