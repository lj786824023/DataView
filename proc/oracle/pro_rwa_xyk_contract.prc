CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_XYK_CONTRACT(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_XYK_CONTRACT
    ʵ�ֹ���:���ÿ�ϵͳ-��ͬ(������Դ����ϵͳ�����׶��������Ϣȫ������RW���ÿ��ӿڱ��ͬ����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :SHUXD
    ��дʱ��:2016-04-22
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.CCS_ACCT|����Ҵ����ʻ���
    Դ  ��2 :RWA_DEV.CCS_ACCA|�˻����Ӽ�¼��
    Դ  ��3 :RWA_DEV.CMS_CUSTOMER_INFO|�ͻ���Ϣ��
    Ŀ���  :RWA_DEV.RWA_XYK_CONTRACT|���ÿ����������
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_XYK_CONTRACT';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_XYK_CONTRACT';

    --2.���������������ݴ�Դ����뵽Ŀ�����-��ʹ�ö�ȡ�δʹ�ö��
    INSERT INTO RWA_DEV.RWA_XYK_CONTRACT(
                DataDate                               --��������
                ,DataNo                                --������ˮ��
                ,ContractID                      	 		 --��ͬID
                ,SContractID                     	 		 --Դ��ͬID
                ,SSysID                          	 		 --ԴϵͳID
                ,ClientID                        	 		 --��������ID
                ,SOrgID                          	 		 --Դ����ID
                ,SOrgName                        	 		 --Դ��������
                ,ORGSORTNO                             --�������������
                ,OrgID                           	 		 --��������ID
                ,OrgName                         	 		 --������������
                ,IndustryID                      	 		 --������ҵ����
                ,IndustryName                    	 		 --������ҵ����
                ,BusinessLine                    	 		 --����
                ,AssetType                       	 		 --�ʲ�����
                ,AssetSubType                   	 		 --�ʲ�С��
                ,BusinessTypeID            				 		 --ҵ��Ʒ�ִ���
                ,BusinessTypeName                	 		 --ҵ��Ʒ������
                ,CreditRiskDataType              	 		 --���÷�����������
                ,StartDate                       	 		 --��ʼ����
                ,DueDate                         	 		 --��������
                ,OriginalMaturity                	 		 --ԭʼ����
                ,ResidualM                       	 		 --ʣ������
                ,SettlementCurrency              	 		 --�������
                ,ContractAmount                    		 --��ͬ�ܽ��
                ,NotExtractPart                  	 		 --��ͬδ��ȡ����
                ,UncondCancelFlag                	 		 --�Ƿ����ʱ����������
                ,ABSUAFlag                       	 		 --�ʲ�֤ȯ�������ʲ���ʶ
                ,ABSPoolID                       	 		 --֤ȯ���ʲ���ID
                ,GroupID                         	 		 --������
                ,GUARANTEETYPE												 --��Ҫ������ʽ
                ,ABSPROPORTION                         --�ʲ�֤ȯ������
    )
    SELECT 
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DataDate                 --��������
                ,p_data_dt_str                               AS DataNo                   --������ˮ��
                ,T1.Exposureid                                AS ContractID               --��ͬID
                ,T1.Dueid                                    AS SContractID              --Դ��ͬID
                ,'XYK'                                       AS SSysID                   --ԴϵͳID                 Ĭ�ϣ����ÿ�(XYK)
                ,T1.CLIENTID                                 AS ClientID                --��������ID
                ,'9997'                                  AS SOrgID                   --Դ����ID
                ,'�������йɷ����޹�˾���ÿ�����'                               AS SOrgName                --Դ��������
                ,'1600'                                   AS ORGSORTNO                --�������������
                ,'9997'                                  AS OrgID                    --��������ID
                ,'�������йɷ����޹�˾���ÿ�����'                               AS OrgName                 --������������
                ,''                                          AS IndustryID               --������ҵ����             Ĭ�ϣ���
                ,''                                          AS IndustryName             --������ҵ����             Ĭ�ϣ���
                ,'0301'                                        AS BusinessLine             --����                     ����ҵ��
                ,'115'                                       AS AssetType                --�ʲ�����                 Ĭ��
                ,'11501'                                     AS AssetSubType             --�ʲ�С��                 Ĭ��
                ,'11106010'                                  AS BusinessTypeID           --ҵ��Ʒ�ִ���             ���ÿ�ҵ��
                ,'���ÿ����'                                 AS BusinessTypeName         --ҵ��Ʒ������             �̶�ֵ�����ÿ�
                ,'02'                                        AS CreditRiskDataType       --���÷�����������         �̶�ֵ��һ������
                ,T1.STARTDATE                                AS StartDate                --��ʼ����
                ,T1.DUEDATE                                  AS DueDate                  --��������
                ,0                                           AS OriginalMaturity         --ԭʼ����
                ,0                                           AS ResidualM                --ʣ������
                ,'CNY'                                       AS SettlementCurrency       --�������                 Ĭ�ϣ������
                ,T1.ASSETBALANCE                             AS ContractAmount           --��ͬ�ܽ��
                ,0                                           AS NotExtractPart              --��ͬδ��ȡ����
                ,'0'                                         AS UncondCancelFlag            --�Ƿ����ʱ����������     Ĭ�ϣ���
                ,'0'                                         AS ABSUAFlag                   --�ʲ�֤ȯ�������ʲ���ʶ   Ĭ�ϣ���
                ,''                                          AS ABSPoolID                   --֤ȯ���ʲ���ID           Ĭ�ϣ���
                ,''                                          AS GroupID                     --������                 RWAϵͳ��ֵ
                ,'005'                                       AS GUARANTEETYPE               --��Ҫ������ʽ
                ,NULL                                        AS ABSPROPORTION               --�ʲ�֤ȯ������
    FROM RWA_DEV.RWA_XYK_EXPOSURE T1 WHERE T1.DATANO = p_data_dt_str
    ;
    COMMIT;


    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_XYK_CONTRACT',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_XYK_CONTRACT;

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '��ͬ('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_XYK_CONTRACT;
/

