CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_HG_CONTRACT(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_HG_CONTRACT
    ʵ�ֹ���:����ϵͳ-�ع�-��ͬ��Ϣ(������Դ����ϵͳ����ͬ�����Ϣ������¶��Ϣȫ������RWA�ع��ӿڱ��ͬ��Ϣ����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-04-12
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.RWA_HG_EXPOSURE|����ϵͳ�ع������÷��ձ�¶��
    Ŀ���  :RWA_DEV.RWA_HG_CONTRACT|����ϵͳ�ع����ͬ��Ϣ��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_HG_CONTRACT';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_HG_CONTRACT';

    --2.���������������ݴ�Դ����뵽Ŀ�����
    INSERT INTO RWA_DEV.RWA_HG_CONTRACT(
               DataDate                             --��������
              ,DataNo                               --������ˮ��
              ,ContractID                           --��ͬID
              ,SContractID                          --Դ��ͬID
              ,SSysID                               --ԴϵͳID
              ,ClientID                             --��������ID
              ,SOrgID                               --Դ����ID
              ,SOrgName                             --Դ��������
              ,OrgSortNo                            --�������������
              ,OrgID                                --��������ID
              ,OrgName                              --������������
              ,IndustryID                           --������ҵ����
              ,IndustryName                         --������ҵ����
              ,BusinessLine                         --ҵ������
              ,AssetType                            --�ʲ�����
              ,AssetSubType                         --�ʲ�С��
              ,BusinessTypeID                       --ҵ��Ʒ�ִ���
              ,BusinessTypeName                     --ҵ��Ʒ������
              ,CreditRiskDataType                   --���÷�����������
              ,StartDate                            --��ʼ����
              ,DueDate                              --��������
              ,OriginalMaturity                     --ԭʼ����
              ,ResidualM                            --ʣ������
              ,SettlementCurrency                   --�������
              ,ContractAmount                       --��ͬ�ܽ��
              ,NotExtractPart                       --��ͬδ��ȡ����
							,UncondCancelFlag  									  --�Ƿ����ʱ����������
							,ABSUAFlag         									  --�ʲ�֤ȯ�������ʲ���ʶ
							,ABSPoolID         									  --֤ȯ���ʲ���ID
							,GroupID           									  --������
							,GUARANTEETYPE     									  --��Ҫ������ʽ
							,ABSPROPORTION												--�ʲ�֤ȯ������
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DataDate                             --��������
                ,p_data_dt_str													     AS DataNo                               --������ˮ��
                ,T1.CONTRACTID                   				 		 AS ContractID                           --��ͬID
                ,T1.CONTRACTID                               AS SContractID                          --Դ��ͬID
                ,T1.SSysID                                   AS SSysID                               --ԴϵͳID
                ,T1.CLIENTID                         			 	 AS ClientID                             --��������ID
                ,T1.SORGID                         					 AS SOrgID                               --Դ����ID
                ,T1.SORGNAME                              	 AS SOrgName                             --Դ��������
                ,T1.ORGSORTNO                                AS OrgSortNo                            --�������������
                ,T1.ORGID                              			 AS OrgID                                --��������ID
                ,T1.ORGNAME                                  AS OrgName                              --������������
                ,T1.INDUSTRYID                               AS IndustryID                           --������ҵ����
                ,T1.INDUSTRYNAME                             AS IndustryName                         --������ҵ����
                ,T1.BusinessLine                             AS BusinessLine                         --ҵ������              				Ĭ�� ͬҵ(04)
                ,T1.ASSETTYPE                              	 AS AssetType                            --�ʲ�����
                ,T1.ASSETSUBTYPE              							 AS AssetSubType                         --�ʲ�С��
                ,T1.BUSINESSTYPEID                           AS BusinessTypeID                       --ҵ��Ʒ�ִ���
                ,T1.BUSINESSTYPENAME                         AS BusinessTypeName                     --ҵ��Ʒ������
                ,T1.CREDITRISKDATATYPE                       AS CreditRiskDataType                   --���÷�����������
                ,T1.STARTDATE                                AS StartDate                            --��ʼ����
                ,T1.DUEDATE                                  AS DueDate                              --��������
                ,T1.ORIGINALMATURITY                         AS OriginalMaturity                     --ԭʼ����
                ,T1.RESIDUALM                                AS ResidualM                            --ʣ������
                ,T1.CURRENCY                                 AS SettlementCurrency                   --�������
                ,T1.NORMALPRINCIPAL                          AS ContractAmount                       --��ͬ�ܽ��
                ,0                                           AS NotExtractPart                       --��ͬδ��ȡ����        				Ĭ�� 0
                ,'0'                                         AS UncondCancelFlag  									 --�Ƿ����ʱ����������  				Ĭ�� ��(0)
                ,'0'                                         AS ABSUAFlag         									 --�ʲ�֤ȯ�������ʲ���ʶ				Ĭ�� ��(0)
                ,''                                        	 AS ABSPoolID         									 --֤ȯ���ʲ���ID        				Ĭ�� ��
                ,''                                          AS GroupID           									 --������              				Ĭ�� ��
                ,''																					 AS GUARANTEETYPE     									 --��Ҫ������ʽ
                ,NULL																				 AS ABSPROPORTION												 --�ʲ�֤ȯ������

    FROM				RWA_DEV.RWA_HG_EXPOSURE T1
    WHERE T1.DATANO=p_data_dt_str
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_HG_CONTRACT',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_HG_CONTRACT;
    --Dbms_output.Put_line('RWA_DEV.RWA_HG_CONTRACT��ǰ����ĺ���ϵͳ-�ع����ݼ�¼Ϊ: ' || v_count || ' ��');




    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '��ͬ��Ϣ('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_HG_CONTRACT;
/

