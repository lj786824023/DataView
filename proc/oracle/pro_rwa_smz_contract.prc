CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_SMZ_CONTRACT(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:PRO_RWA_SMZ_CONTRACT
    ʵ�ֹ���:˽ļծ-��ͬ
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :SHUXD
    ��дʱ��:2016-07-08
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_SMZ_EXPOSURE|˽ļծ��¶��
    Ŀ���  :RWA_SMZ_CONTRACT|˽ļծ��ͬ��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'PRO_RWA_SMZ_CONTRACT';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_SMZ_CONTRACT';

    --2.���������������ݲ��뵽Ŀ�����
    INSERT INTO RWA_SMZ_CONTRACT(
                DataDate                               --��������
                ,DataNo                                --������ˮ��
                ,ContractID                      	 		 --��ͬID
                ,SContractID                     	 		 --Դ��ͬID
                ,SSysID                          	 		 --ԴϵͳID
                ,ClientID                        	 		 --��������ID
                ,SOrgID                          	 		 --Դ����ID
                ,SOrgName                        	 		 --Դ��������
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
                ,ABSProportion                   	 		 --�ʲ�֤ȯ������
                ,GroupID                         	 		 --������
                ,GUARANTEETYPE												 --��Ҫ������ʽ
                ,ORGSORTNO                             --�������������
    )WITH TEMP_WS_PRIVATE_BOND AS (
                  SELECT ZYDBFSDM,ZQID
                    FROM RWA.RWA_WS_PRIVATE_BOND WHERE ROWID IN(
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
              )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DataDate                 --��������
                ,p_data_dt_str													     AS DataNo                   --������ˮ��
                ,T1.ContractID                               AS ContractID            	 --��ͬID
                ,T1.ContractID                        	 		 AS SContractID        	 	 	 --Դ��ͬID
                ,'SMZ'                                       AS SSysID                	 --ԴϵͳID
                ,T1.ClientID                       			     AS ClientID              	 --��������ID
                ,T1.OrgID                          			     AS SOrgID                	 --Դ����ID
                ,T1.OrgName                              	   AS SOrgName            	 	 --Դ��������
                ,T1.OrgID                               	   AS OrgID                 	 --��������ID
                ,T1.OrgName                                  AS OrgName               	 --������������
                ,T1.IndustryID                               AS IndustryID            	 --������ҵ����
                ,T1.IndustryName                             AS IndustryName          	 --������ҵ����
                ,''                              		         AS BusinessLine          	 --����
                ,''                                          AS AssetType                --�ʲ�����
                ,''                                          AS AssetSubType             --�ʲ�С��
                ,''                                          AS BusinessTypeID           --ҵ��Ʒ�ִ���
                ,''                                          AS BusinessTypeName         --ҵ��Ʒ������
                ,'06'                                        AS CreditRiskDataType       --���÷�����������
                ,T1.StartDate                                AS StartDate                --��ʼ����
                ,T1.DueDate                                  AS DueDate                  --��������
                ,T1.OriginalMaturity                         AS OriginalMaturity         --ԭʼ����
                ,T1.ResidualM                                AS ResidualM                --ʣ������
                ,T1.CURRENCY                                 AS SettlementCurrency       --�������
                ,''                                          AS ContractAmount           --��ͬ�ܽ��
                ,''                                          AS NotExtractPart           --��ͬδ��ȡ����
                ,'0'                                         AS UncondCancelFlag         --�Ƿ����ʱ����������
                ,'0'                                         AS ABSUAFlag                --�ʲ�֤ȯ�������ʲ���ʶ
                ,''                                          AS ABSPoolID                --֤ȯ���ʲ���ID
                ,0                                           AS ABSProportion            --�ʲ�֤ȯ������
                ,''                                          AS GroupID                  --������
                ,T2.ZYDBFSDM                                 AS GUARANTEETYPE            --��Ҫ������ʽ
                ,T1.ORGSORTNO                                AS ORGSORTNO                --�������������

    FROM        RWA_DEV.RWA_SMZ_EXPOSURE T1
    LEFT JOIN   TEMP_WS_PRIVATE_BOND T2
    ON          T1.CONTRACTID=T2.ZQID
    WHERE       T1.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_SMZ_CONTRACT;
    --Dbms_output.Put_line('RWA_SMZ_CONTRACT��ǰ��������ÿ�ϵͳ���ݼ�¼Ϊ: ' || v_count || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
          --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '��ͬ('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_SMZ_CONTRACT;
/

