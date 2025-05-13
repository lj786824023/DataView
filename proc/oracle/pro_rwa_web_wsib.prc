CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_WEB_WSIB(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_WEB_WSIB
    ʵ�ֹ���:RWAϵͳ-ҳ�油¼���-��¼�̵�(������ԴRWAϵͳ��ҵ�������Ϣȫ������RWAҳ����ز�¼����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-06-20
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.RWA_EI_UNCONSFIINVEST|��ȨͶ����ϸ��
    Դ  ��2 :RWA_DEV.RWA_EI_PROFITDIST|������䷽����
    Դ  ��3 :RWA_DEV.RWA_EI_TAXASSET|������˰��Ϣ��
    Դ  ��4 :RWA_DEV.RWA_EI_FAILEDTTC|�����ʱ����߱�
    Ŀ���1 :RWA_DEV.RWA_EI_UNCONSFIINVEST|��ȨͶ����ϸ��
    Ŀ���2 :RWA_DEV.RWA_EI_PROFITDIST|������䷽����
    Ŀ���3 :RWA_DEV.RWA_EI_TAXASSET|������˰��Ϣ��
    Ŀ���4 :RWA_DEV.RWA_EI_FAILEDTTC|�����ʱ����߱�
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_WEB_WSIB';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count1 INTEGER;
  v_count2 INTEGER;
  v_count3 INTEGER;
  v_count4 INTEGER;

  v_cur_cnt1 INTEGER;
  v_cur_cnt2 INTEGER;
  v_cur_cnt3 INTEGER;
  v_cur_cnt4 INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.ͳ��Ŀ����еĵ��ڼ�¼
    --��ȨͶ����ϸ��
    SELECT COUNT(1) INTO v_cur_cnt1 FROM RWA_DEV.RWA_EI_UNCONSFIINVEST WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    --������䷽����
    SELECT COUNT(1) INTO v_cur_cnt2 FROM RWA_DEV.RWA_EI_PROFITDIST WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    --������˰��Ϣ��
    SELECT COUNT(1) INTO v_cur_cnt3 FROM RWA_DEV.RWA_EI_TAXASSET WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    --�����ʱ����߱�
    SELECT COUNT(1) INTO v_cur_cnt4 FROM RWA_DEV.RWA_EI_FAILEDTTC WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1 RWAϵͳ-��ȨͶ����ϸ��
    INSERT INTO RWA_DEV.RWA_EI_UNCONSFIINVEST(
                SERIALNO                                 --��ˮ��
                ,DATADATE                             	 --��������
                ,DATANO             										 --������ˮ��
                ,INVESTEENAME                            --��Ͷ�ʵ�λ����
                ,ORGANIZATIONCODE                     	 --Ͷ�ʶ�����֯��������
                ,EQUITYINVESTTYPE                     	 --��ȨͶ�ʶ�������
                ,ORGID                                	 --�ֹɻ���
                ,EQUITYINVESTAMOUNT                   	 --δ������ڽ��ڻ�����ȨͶ�ʽ��(δ�۳�����)
                ,CTOCINVESTAMOUNT   										 --����һ���ʱ�Ͷ�ʽ��
                ,OTOCINVESTAMOUNT                        --����һ���ʱ�Ͷ�ʽ��
                ,TTCINVESTAMOUNT                         --�����ʱ�Ͷ�ʽ��
                ,CTOCGAP                                 --����һ���ʱ�ȱ��
                ,OTOCGAP                                 --����һ���ʱ�ȱ��
                ,TTCGAP                                  --�����ʱ�ȱ��
                ,PROVISIONS                              --��ֵ׼��
                ,EQUITYINVESTEEPROP                      --ռ��Ͷ�ʵ�λȨ�����
                ,SUBJECT                                 --��Ŀ
                ,CURRENCY                                --����
                ,EQUITYNATURE                            --��Ȩ����
                ,EQUITYINVESTCAUSE                       --��ȨͶ���γ�ԭ��
                ,CONSOLIDATEFLAG                         --�Ƿ����벢��Χ
                ,NOTCONSOLIDATECAUSE                     --�����벢���ԭ��
                ,RISKCLASSIFY                            --���շ���
                ,BUSINESSLINE                            --����
                ,INPUTUSERID                             --�Ǽ���ID
                ,INPUTORGID                              --�Ǽǻ���ID
                ,INPUTTIME                               --�Ǽ�ʱ��
                ,UPDATEUSERID                            --������ID
                ,UPDATEORGID                             --���»���ID
                ,UPDATETIME                              --����ʱ��
                ,CUSTID1                                 --�ͻ����
    )
    SELECT
                p_data_dt_str || lpad(rownum, 8, '0')    AS SERIALNO                      --��ˮ��
                ,TO_DATE(p_data_dt_str,'YYYYMMDD')     	 AS DATADATE                      --��������
                ,p_data_dt_str       										 AS DATANO                        --������ˮ��
                ,INVESTEENAME                            AS INVESTEENAME                  --��Ͷ�ʵ�λ����
                ,ORGANIZATIONCODE                     	 AS ORGANIZATIONCODE              --Ͷ�ʶ�����֯��������
                ,EQUITYINVESTTYPE                     	 AS EQUITYINVESTTYPE              --��ȨͶ�ʶ�������
                ,ORGID                                	 AS ORGID                         --�ֹɻ���
                ,EQUITYINVESTAMOUNT                   	 AS EQUITYINVESTAMOUNT            --δ������ڽ��ڻ�����ȨͶ�ʽ��(δ�۳�����)
                ,CTOCINVESTAMOUNT   										 AS CTOCINVESTAMOUNT              --����һ���ʱ�Ͷ�ʽ��
                ,OTOCINVESTAMOUNT                        AS OTOCINVESTAMOUNT              --����һ���ʱ�Ͷ�ʽ��
                ,TTCINVESTAMOUNT                         AS TTCINVESTAMOUNT               --�����ʱ�Ͷ�ʽ��
                ,CTOCGAP                                 AS CTOCGAP                       --����һ���ʱ�ȱ��
                ,OTOCGAP                                 AS OTOCGAP                       --����һ���ʱ�ȱ��
                ,TTCGAP                                  AS TTCGAP                        --�����ʱ�ȱ��
                ,PROVISIONS                              AS PROVISIONS                    --��ֵ׼��
                ,EQUITYINVESTEEPROP                      AS EQUITYINVESTEEPROP            --ռ��Ͷ�ʵ�λȨ�����
                ,SUBJECT                                 AS SUBJECT                       --��Ŀ
                ,CURRENCY                                AS CURRENCY                      --����
                ,EQUITYNATURE                            AS EQUITYNATURE                  --��Ȩ����
                ,EQUITYINVESTCAUSE                       AS EQUITYINVESTCAUSE             --��ȨͶ���γ�ԭ��
                ,CONSOLIDATEFLAG                         AS CONSOLIDATEFLAG               --�Ƿ����벢��Χ
                ,NOTCONSOLIDATECAUSE                     AS NOTCONSOLIDATECAUSE           --�����벢���ԭ��
                ,RISKCLASSIFY                            AS RISKCLASSIFY                  --���շ���
                ,BUSINESSLINE                            AS BUSINESSLINE                  --����
                ,INPUTUSERID                             AS INPUTUSERID                   --�Ǽ���ID
                ,INPUTORGID                              AS INPUTORGID                    --�Ǽǻ���ID
                ,INPUTTIME                               AS INPUTTIME                     --�Ǽ�ʱ��
                ,UPDATEUSERID                            AS UPDATEUSERID                  --������ID
                ,UPDATEORGID                             AS UPDATEORGID                   --���»���ID
                ,UPDATETIME                              AS UPDATETIME                    --����ʱ��
                ,CUSTID1                                 AS CUSTID1                       --�ͻ����

    FROM				RWA_DEV.RWA_EI_UNCONSFIINVEST
		WHERE 			DATADATE = (SELECT MAX(DATADATE) FROM RWA_DEV.RWA_EI_UNCONSFIINVEST WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
		AND					v_cur_cnt1 = 0
		ORDER BY 		SERIALNO
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_UNCONSFIINVEST',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_EI_UNCONSFIINVEST WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD') AND SERIALNO LIKE p_data_dt_str || '________';
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_UNCONSFIINVEST��ǰ�����RWAϵͳ-��ȨͶ����ϸ�̵����ݼ�¼Ϊ: ' || v_count1 || ' ��');


    --2.2 RWAϵͳ-������䷽����
    INSERT INTO RWA_DEV.RWA_EI_PROFITDIST(
                SERIALNO                               --��ˮ��
                ,DATADATE                            	 --��������
                ,DATANO                                --������ˮ��
                ,SBDUDPROFITS       									 --Ӧ��δ������
                ,ILDDEBT                               --�����ʲ�������ʹ��Ȩ��Ӧ���ֵĵ���˰��ծ
                ,SHARESBALANCE                         --�ɷ����
                ,TYBPROFITDISTSHARES                   --ǰһ������������
                ,HOLDINGTIME                           --�ֹ�ʱ��
                ,INPUTUSERID                           --�Ǽ���ID
                ,INPUTORGID                            --�Ǽǻ���ID
                ,INPUTTIME                             --�Ǽ�ʱ��
                ,UPDATEUSERID                          --������ID
                ,UPDATEORGID                           --���»���ID
                ,UPDATETIME                            --����ʱ��
    )
    SELECT
                p_data_dt_str || lpad(rownum, 8, '0')    AS SERIALNO                      --��ˮ��
                ,TO_DATE(p_data_dt_str,'YYYYMMDD')     	 AS DATADATE                      --��������
                ,p_data_dt_str       										 AS DATANO                        --������ˮ��
                ,SBDUDPROFITS                            AS SBDUDPROFITS       						--Ӧ��δ������
                ,ILDDEBT            				             AS ILDDEBT                       --�����ʲ�������ʹ��Ȩ��Ӧ���ֵĵ���˰��ծ
                ,SHARESBALANCE      				             AS SHARESBALANCE                 --�ɷ����
                ,TYBPROFITDISTSHARES                     AS TYBPROFITDISTSHARES           --ǰһ������������
                ,HOLDINGTIME                             AS HOLDINGTIME                   --�ֹ�ʱ��
                ,INPUTUSERID                             AS INPUTUSERID                   --�Ǽ���ID
                ,INPUTORGID                              AS INPUTORGID                    --�Ǽǻ���ID
                ,INPUTTIME          										 AS INPUTTIME                     --�Ǽ�ʱ��
                ,UPDATEUSERID                            AS UPDATEUSERID                  --������ID
                ,UPDATEORGID                             AS UPDATEORGID                   --���»���ID
                ,UPDATETIME                              AS UPDATETIME                    --����ʱ��

    FROM				RWA_DEV.RWA_EI_PROFITDIST
		WHERE 			DATADATE = (SELECT MAX(DATADATE) FROM RWA_DEV.RWA_EI_PROFITDIST WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
		AND					v_cur_cnt2 = 0
		ORDER BY 		SERIALNO
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_PROFITDIST',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count2 FROM RWA_DEV.RWA_EI_PROFITDIST WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD') AND SERIALNO LIKE p_data_dt_str || '________';
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_PROFITDIST��ǰ�����RWAϵͳ-������䷽���̵����ݼ�¼Ϊ: ' || v_count2 || ' ��');

    --2.3 RWAϵͳ-������˰��Ϣ��
    INSERT INTO RWA_DEV.RWA_EI_TAXASSET(
                SERIALNO                                 --��ˮ��
                ,DATADATE                             	 --��������
                ,DATANO             										 --������ˮ��
                ,TAXTYPE                             		 --������˰����
                ,ASSET                            	 		 --������˰�ʲ�
                ,DEBT                             	 		 --������˰��ծ
                ,VALIDATEFLAG                     	 		 --У����
                ,INPUTUSERID                      	 		 --�Ǽ���
                ,INPUTORGID     										 		 --�Ǽǻ���
                ,INPUTTIME                           		 --�Ǽ�ʱ��
                ,UPDATEUSERID                        		 --������
                ,UPDATEORGID                         		 --���»���
                ,UPDATETIME                          		 --����ʱ��
                ,DKZCJZZB_D                          		 --�����ʲ���ֵ��ʧ(ȫ�ھ�+�����ʲ���Ϣ����)�ʲ�
                ,DKZCJZZB_C                          		 --�����ʲ���ֵ��ʧ(ȫ�ھ�+�����ʲ���Ϣ����)��ծ
                ,CQGQTZ_D                            		 --���ڹ�ȨͶ�� �ʲ�
                ,CQGQTZ_C                            		 --���ڹ�ȨͶ�� ��ծ
                ,CFTYKX_D                            		 --���ͬҵ���� �ʲ�
                ,CFTYKX_C                            		 --���ͬҵ���� ��ծ
                ,YSLX_D                              		 --Ӧ����Ϣ �ʲ�
                ,YSLX_C                              		 --Ӧ����Ϣ ��ծ
                ,QTYSK_D                             		 --����Ӧ�տӦ�տ���Ͷ�� �ʲ�
                ,QTYSK_C                             		 --����Ӧ�տӦ�տ���Ͷ�� ��ծ
                ,DZZC_D                              		 --��ծ�ʲ� �ʲ�
                ,DZZC_C                              		 --��ծ�ʲ� ��ծ
                ,JYXJRZCGYJZBD_D                     		 --�����Խ����ʲ������ʼ�ֵ�䶯�� �ʲ�
                ,JYXJRZCGYJZBD_C                     		 --�����Խ����ʲ������ʼ�ֵ�䶯�� ��ծ
                ,JYXJRZC_D                           		 --�����Խ����ʲ� �ʲ�
                ,JYXJRZC_C                           		 --�����Խ����ʲ� ��ծ
                ,ZQTZGYJZBD_D                        		 --�ɹ����۽����ʲ�-ծȯͶ��(���ʼ�ֵ�䶯) �ʲ�
                ,ZQTZGYJZBD_C                        		 --�ɹ����۽����ʲ�-ծȯͶ��(���ʼ�ֵ�䶯) ��ծ
                ,ZQTZ_D         												 --�ɹ����۽����ʲ�-ծȯͶ�� �ʲ�
                ,ZQTZ_C                                  --�ɹ����۽����ʲ�-ծȯͶ�� ��ծ
                ,QTGQGYJZBD_D                            --�ɹ����۽����ʲ�-(������Ȩ���ʼ�ֵ�䶯) �ʲ�
                ,QTGQGYJZBD_C                            --�ɹ����۽����ʲ�-(������Ȩ���ʼ�ֵ�䶯) ��ծ
                ,QTGQ_D                                  --�ɹ����۽����ʲ�-������Ȩ �ʲ�
                ,QTGQ_C                                  --�ɹ����۽����ʲ�-������Ȩ ��ծ
                ,CYZDQJRZC_D                             --���������ڽ����ʲ� �ʲ�
                ,CYZDQJRZC_C                             --���������ڽ����ʲ� ��ծ
                ,YJFZ_D                                  --Ԥ�Ƹ�ծ-Ӧ������ְ��н�� �ʲ�
                ,YJFZ_C                                  --Ԥ�Ƹ�ծ-Ӧ������ְ��н�� ��ծ
                ,YFGZJJJ_D                               --Ӧ�����ʼ����� �ʲ�
                ,YFGZJJJ_C                               --Ӧ�����ʼ����� ��ծ
                ,YFZFGJJ_D                               --Ӧ��ס�������� �ʲ�
                ,YFZFGJJ_C                               --Ӧ��ס�������� ��ծ
                ,YFNJ_D                                  --Ӧ����� �ʲ�
                ,YFNJ_C                                  --Ӧ����� ��ծ
                ,YFGHJF_D                                --Ӧ�����ᾭ�� �ʲ�
                ,YFGHJF_C                                --Ӧ�����ᾭ�� ��ծ
                ,YFJBSHBX_D                              --Ӧ��������ᱣ�� �ʲ�
                ,YFJBSHBX_C                              --Ӧ��������ᱣ�� ��ծ
                ,YSLCSXFSRZSK_D                          --Ӧ������������������տ��ʲ�
                ,YSLCSXFSRZSK_C                          --Ӧ������������������տծ
                ,ZSCWGWF_D                               --���ղ�����ʷ��ʲ�
                ,ZSCWGWF_C                               --���ղ�����ʷѸ�ծ
                ,QTYSKQT_D                               --����Ӧ�տ�-�����ʲ�
                ,QTYSKQT_C                               --����Ӧ�տ�-������ծ
                ,MRFSJRZCLXTZ_D                          --���뷵�۽����ʲ�-��Ϣ�����ʲ�
                ,MRFSJRZCLXTZ_C                          --���뷵�۽����ʲ�-��Ϣ������ծ
                ,TXZCLXTZ_D                              --�����ʲ�-��Ϣ�����ʲ�
                ,TXZCLXTZ_C                              --�����ʲ�-��Ϣ������ծ
                ,MCHGJRZCLXTZ_D                          --�����ع������ʲ�-��Ϣ�����ʲ�
                ,MCHGJRZCLXTZ_C                          --�����ع������ʲ�-��Ϣ������ծ
                ,WAQZFSZQRCB_D                           --δ��Ȩ������ȷ�ϳɱ��ʲ�
                ,WAQZFSZQRCB_C                           --δ��Ȩ������ȷ�ϳɱ���ծ
    )
    SELECT
                p_data_dt_str || lpad(rownum, 8, '0')    AS SERIALNO                      --��ˮ��
                ,TO_DATE(p_data_dt_str,'YYYYMMDD')     	 AS DATADATE                      --��������
                ,p_data_dt_str       										 AS DATANO                        --������ˮ��
                ,TAXTYPE                             		 AS TAXTYPE                   		--������˰����
                ,ASSET                            	 		 AS ASSET                     		--������˰�ʲ�
                ,DEBT                             	 		 AS DEBT                      		--������˰��ծ
                ,VALIDATEFLAG                     	 		 AS VALIDATEFLAG              		--У����
                ,INPUTUSERID                      	 		 AS INPUTUSERID               		--�Ǽ���
                ,INPUTORGID     										 		 AS INPUTORGID                		--�Ǽǻ���
                ,INPUTTIME                           		 AS INPUTTIME                 		--�Ǽ�ʱ��
                ,UPDATEUSERID                        		 AS UPDATEUSERID              		--������
                ,UPDATEORGID                         		 AS UPDATEORGID               		--���»���
                ,UPDATETIME                          		 AS UPDATETIME                		--����ʱ��
                ,DKZCJZZB_D                          		 AS DKZCJZZB_D                		--�����ʲ���ֵ��ʧ(ȫ�ھ�+�����ʲ���Ϣ����)�ʲ�
                ,DKZCJZZB_C                          		 AS DKZCJZZB_C                		--�����ʲ���ֵ��ʧ(ȫ�ھ�+�����ʲ���Ϣ����)��ծ
                ,CQGQTZ_D                            		 AS CQGQTZ_D                  		--���ڹ�ȨͶ�� �ʲ�
                ,CQGQTZ_C                            		 AS CQGQTZ_C                  		--���ڹ�ȨͶ�� ��ծ
                ,CFTYKX_D                            		 AS CFTYKX_D                  		--���ͬҵ���� �ʲ�
                ,CFTYKX_C                            		 AS CFTYKX_C                  		--���ͬҵ���� ��ծ
                ,YSLX_D                              		 AS YSLX_D                    		--Ӧ����Ϣ �ʲ�
                ,YSLX_C                              		 AS YSLX_C                    		--Ӧ����Ϣ ��ծ
                ,QTYSK_D                             		 AS QTYSK_D                   		--����Ӧ�տӦ�տ���Ͷ�� �ʲ�
                ,QTYSK_C                             		 AS QTYSK_C                   		--����Ӧ�տӦ�տ���Ͷ�� ��ծ
                ,DZZC_D                              		 AS DZZC_D                    		--��ծ�ʲ� �ʲ�
                ,DZZC_C                              		 AS DZZC_C                    		--��ծ�ʲ� ��ծ
                ,JYXJRZCGYJZBD_D                     		 AS JYXJRZCGYJZBD_D           		--�����Խ����ʲ������ʼ�ֵ�䶯�� �ʲ�
                ,JYXJRZCGYJZBD_C                     		 AS JYXJRZCGYJZBD_C           		--�����Խ����ʲ������ʼ�ֵ�䶯�� ��ծ
                ,JYXJRZC_D                           		 AS JYXJRZC_D                 		--�����Խ����ʲ� �ʲ�
                ,JYXJRZC_C                           		 AS JYXJRZC_C                 		--�����Խ����ʲ� ��ծ
                ,ZQTZGYJZBD_D                        		 AS ZQTZGYJZBD_D              		--�ɹ����۽����ʲ�-ծȯͶ��(���ʼ�ֵ�䶯) �ʲ�
                ,ZQTZGYJZBD_C                        		 AS ZQTZGYJZBD_C              		--�ɹ����۽����ʲ�-ծȯͶ��(���ʼ�ֵ�䶯) ��ծ
                ,ZQTZ_D         												 AS ZQTZ_D         								--�ɹ����۽����ʲ�-ծȯͶ�� �ʲ�
                ,ZQTZ_C                                  AS ZQTZ_C                        --�ɹ����۽����ʲ�-ծȯͶ�� ��ծ
                ,QTGQGYJZBD_D                            AS QTGQGYJZBD_D                  --�ɹ����۽����ʲ�-(������Ȩ���ʼ�ֵ�䶯) �ʲ�
                ,QTGQGYJZBD_C                            AS QTGQGYJZBD_C                  --�ɹ����۽����ʲ�-(������Ȩ���ʼ�ֵ�䶯) ��ծ
                ,QTGQ_D                                  AS QTGQ_D                        --�ɹ����۽����ʲ�-������Ȩ �ʲ�
                ,QTGQ_C                                  AS QTGQ_C                        --�ɹ����۽����ʲ�-������Ȩ ��ծ
                ,CYZDQJRZC_D                             AS CYZDQJRZC_D                   --���������ڽ����ʲ� �ʲ�
                ,CYZDQJRZC_C                             AS CYZDQJRZC_C                   --���������ڽ����ʲ� ��ծ
                ,YJFZ_D                                  AS YJFZ_D                        --Ԥ�Ƹ�ծ-Ӧ������ְ��н�� �ʲ�
                ,YJFZ_C                                  AS YJFZ_C                        --Ԥ�Ƹ�ծ-Ӧ������ְ��н�� ��ծ
                ,YFGZJJJ_D                               AS YFGZJJJ_D                     --Ӧ�����ʼ����� �ʲ�
                ,YFGZJJJ_C                               AS YFGZJJJ_C                     --Ӧ�����ʼ����� ��ծ
                ,YFZFGJJ_D                               AS YFZFGJJ_D                     --Ӧ��ס�������� �ʲ�
                ,YFZFGJJ_C                               AS YFZFGJJ_C                     --Ӧ��ס�������� ��ծ
                ,YFNJ_D                                  AS YFNJ_D                        --Ӧ����� �ʲ�
                ,YFNJ_C                                  AS YFNJ_C                        --Ӧ����� ��ծ
                ,YFGHJF_D                                AS YFGHJF_D                      --Ӧ�����ᾭ�� �ʲ�
                ,YFGHJF_C                                AS YFGHJF_C                      --Ӧ�����ᾭ�� ��ծ
                ,YFJBSHBX_D                              AS YFJBSHBX_D                    --Ӧ��������ᱣ�� �ʲ�
                ,YFJBSHBX_C                              AS YFJBSHBX_C                    --Ӧ��������ᱣ�� ��ծ
                ,YSLCSXFSRZSK_D                          AS YSLCSXFSRZSK_D                --Ӧ������������������տ��ʲ�
                ,YSLCSXFSRZSK_C                          AS YSLCSXFSRZSK_C                --Ӧ������������������տծ
                ,ZSCWGWF_D                               AS ZSCWGWF_D                     --���ղ�����ʷ��ʲ�
                ,ZSCWGWF_C                               AS ZSCWGWF_C                     --���ղ�����ʷѸ�ծ
                ,QTYSKQT_D                               AS QTYSKQT_D                     --����Ӧ�տ�-�����ʲ�
                ,QTYSKQT_C                               AS QTYSKQT_C                     --����Ӧ�տ�-������ծ
                ,MRFSJRZCLXTZ_D                          AS MRFSJRZCLXTZ_D                --���뷵�۽����ʲ�-��Ϣ�����ʲ�
                ,MRFSJRZCLXTZ_C                          AS MRFSJRZCLXTZ_C                --���뷵�۽����ʲ�-��Ϣ������ծ
                ,TXZCLXTZ_D                              AS TXZCLXTZ_D                    --�����ʲ�-��Ϣ�����ʲ�
                ,TXZCLXTZ_C                              AS TXZCLXTZ_C                    --�����ʲ�-��Ϣ������ծ
                ,MCHGJRZCLXTZ_D                          AS MCHGJRZCLXTZ_D                --�����ع������ʲ�-��Ϣ�����ʲ�
                ,MCHGJRZCLXTZ_C                          AS MCHGJRZCLXTZ_C                --�����ع������ʲ�-��Ϣ������ծ
                ,WAQZFSZQRCB_D                           AS WAQZFSZQRCB_D                 --δ��Ȩ������ȷ�ϳɱ��ʲ�
                ,WAQZFSZQRCB_C                           AS WAQZFSZQRCB_C                 --δ��Ȩ������ȷ�ϳɱ���ծ

    FROM				RWA_DEV.RWA_EI_TAXASSET
		WHERE 			DATADATE = (SELECT MAX(DATADATE) FROM RWA_DEV.RWA_EI_TAXASSET WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
		AND					v_cur_cnt3 = 0
		ORDER BY 		SERIALNO
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_TAXASSET',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count3 FROM RWA_DEV.RWA_EI_TAXASSET WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD') AND SERIALNO LIKE p_data_dt_str || '________';
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_TAXASSET��ǰ�����RWAϵͳ-������˰��Ϣϸ�̵����ݼ�¼Ϊ: ' || v_count3 || ' ��');

    --2.4 RWAϵͳ-�����ʱ����߱�
    INSERT INTO RWA_DEV.RWA_EI_FAILEDTTC(
                SERIALNO                                 --��ˮ��
                ,DATADATE                             	 --��������
                ,DATANO             										 --������ˮ��
                ,BONDNAME                           		 --ծȯ����
                ,DENOMINATION                    	  		 --���
                ,BOOKBALANCE                     	  		 --�������
                ,VALUEDATE                       	  		 --��Ϣ��
                ,REDEMPTIONDATE                  	  		 --�����
                ,HONOURDATE    										  		 --�Ҹ���
                ,BONDCLASSIFY                       		 --ծȯ����
                ,RESIDUALM                          		 --ʣ������
                ,INPUTUSERID                        		 --�Ǽ���ID
                ,INPUTORGID                         		 --�Ǽǻ���ID
                ,INPUTTIME                          		 --�Ǽ�ʱ��
                ,UPDATEUSERID                       		 --������ID
                ,UPDATEORGID                        		 --���»���ID
                ,UPDATETIME                         		 --����ʱ��
                ,QUALFLAG                           		 --�Ƿ�ϸ�
    )
    SELECT
                p_data_dt_str || lpad(rownum, 8, '0')    AS SERIALNO                      --��ˮ��
                ,TO_DATE(p_data_dt_str,'YYYYMMDD')     	 AS DATADATE                      --��������
                ,p_data_dt_str       										 AS DATANO                        --������ˮ��
                ,BONDNAME                           		 AS BONDNAME                  		--ծȯ����
                ,DENOMINATION                    	  		 AS DENOMINATION              		--���
                ,BOOKBALANCE                     	  		 AS BOOKBALANCE               		--�������
                ,VALUEDATE                       	  		 AS VALUEDATE                 		--��Ϣ��
                ,REDEMPTIONDATE                  	  		 AS REDEMPTIONDATE            		--�����
                ,HONOURDATE    										  		 AS HONOURDATE                		--�Ҹ���
                ,BONDCLASSIFY                       		 AS BONDCLASSIFY              		--ծȯ����
                ,RESIDUALM                          		 AS RESIDUALM                 		--ʣ������
                ,INPUTUSERID                        		 AS INPUTUSERID               		--�Ǽ���ID
                ,INPUTORGID                         		 AS INPUTORGID                		--�Ǽǻ���ID
                ,INPUTTIME                          		 AS INPUTTIME                 		--�Ǽ�ʱ��
                ,UPDATEUSERID                       		 AS UPDATEUSERID              		--������ID
                ,UPDATEORGID                        		 AS UPDATEORGID               		--���»���ID
                ,UPDATETIME                         		 AS UPDATETIME                		--����ʱ��
                ,QUALFLAG                           		 AS QUALFLAG                  		--�Ƿ�ϸ�

    FROM				RWA_DEV.RWA_EI_FAILEDTTC
		WHERE 			DATADATE = (SELECT MAX(DATADATE) FROM RWA_DEV.RWA_EI_FAILEDTTC WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
		AND					v_cur_cnt4 = 0
		ORDER BY 		SERIALNO
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_FAILEDTTC',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count4 FROM RWA_DEV.RWA_EI_FAILEDTTC WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD') AND SERIALNO LIKE p_data_dt_str || '________';
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_FAILEDTTC��ǰ�����RWAϵͳ-�����ʱ�������Ϣ�̵����ݼ�¼Ϊ: ' || v_count4 || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || (v_count1 + v_count2 + v_count3 + v_count4);
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := 'ҳ�油¼�����̵�('|| v_pro_name ||')����ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_WEB_WSIB;
/

