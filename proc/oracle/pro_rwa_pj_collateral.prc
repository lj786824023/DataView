CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_PJ_COLLATERAL(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_PJ_COLLATERAL
    ʵ�ֹ���:����ϵͳ-Ʊ������-����ѺƷ(������Դ����ϵͳ��Ʊ�����������Ϣȫ������RWAƱ�����ֽӿڱ����ѺƷ����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-04-21
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.BRD_BILL|Ʊ����Ϣ
    Ŀ���  :RWA_DEV.RWA_PJ_COLLATERAL|Ʊ�����������ѺƷ��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    pxl 2019/04/16 ȥ����ز�¼���Ϻ��ı�
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_PJ_COLLATERAL';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_PJ_COLLATERAL';

    /*��Ч����º�ͬ��Ӧ�ĵ���ѺƷ��Ϣ��ת����_��ת��*/
    INSERT INTO RWA_DEV.RWA_PJ_COLLATERAL(
                 DATADATE                                --��������
                ,DATANO                                 --������ˮ��
                ,COLLATERALID                           --����ѺƷID
                ,SSYSID                                 --ԴϵͳID
                ,SGUARCONTRACTID                        --Դ������ͬID
                ,SCOLLATERALID                          --Դ����ѺƷID
                ,COLLATERALNAME                         --����ѺƷ����
                ,ISSUERID                               --������ID
                ,PROVIDERID                             --�ṩ��ID
                ,CREDITRISKDATATYPE                     --���÷�����������
                ,GUARANTEEWAY                            --������ʽ
                ,SOURCECOLTYPE                          --Դ����ѺƷ����
                ,SOURCECOLSUBTYPE                       --Դ����ѺƷС��
                ,SPECPURPBONDFLAG                       --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,QUALFLAGSTD                            --Ȩ�ط��ϸ��ʶ
                ,QUALFLAGFIRB                           --�����������ϸ��ʶ
                ,COLLATERALTYPESTD                      --Ȩ�ط�����ѺƷ����
                ,COLLATERALSDVSSTD                      --Ȩ�ط�����ѺƷϸ��
                ,COLLATERALTYPEIRB                      --����������ѺƷ����
                ,COLLATERALAMOUNT                        --��Ѻ�ܶ�
                ,CURRENCY                               --����
                ,STARTDATE                              --��ʼ����
                ,DUEDATE                                --��������
                ,ORIGINALMATURITY                       --ԭʼ����
                ,RESIDUALM                              --ʣ������
                ,INTEHAIRCUTSFLAG                       --���й����ۿ�ϵ����ʶ
                ,INTERNALHC                             --�ڲ��ۿ�ϵ��
                ,FCTYPE                                 --������ѺƷ����
                ,ABSFLAG                                --�ʲ�֤ȯ����ʶ
                ,RATINGDURATIONTYPE                     --������������
                ,FCISSUERATING                          --������ѺƷ���еȼ�
                ,FCISSUERTYPE                           --������ѺƷ���������
                ,FCISSUERSTATE                          --������ѺƷ������ע�����
                ,FCRESIDUALM                            --������ѺƷʣ������
                ,REVAFREQUENCY                          --�ع�Ƶ��
                ,GROUPID                                --������
                ,RCERating                              --�����˾���ע����ⲿ����
    )
    SELECT
          TO_DATE(p_data_dt_str,'YYYYMMDD') , --��������        
          p_data_dt_str , --������ˮ��       
          'PJ' || T1.CRDT_BIZ_ID  , --����ѺƷID        
          'PJ'  , --ԴϵͳID       
          'PJ' || T1.CRDT_BIZ_ID  , --Դ������ͬID       
          'PJ' || T1.CRDT_BIZ_ID  , --Դ����ѺƷID       
          CASE WHEN SUBSTR(T1.SBJT_CD, 1, 6)='130102' THEN '��ҵ�жһ�Ʊ'
               ELSE '�й���ҵ���гжһ�Ʊ' 
          END, --����ѺƷ����        
          T1.CUST_NO , --������ID       ��������֤
          T1.CUST_NO , --�ṩ��ID       ��������֤
          '01'  , --���÷�����������        
          '060' , --������ʽ        
          '001004'  , --Դ����ѺƷ����       
          CASE WHEN SUBSTR(T1.SBJT_CD, 1, 6)='130102' THEN '001004004001'
               ELSE '001004002001'  
          END , --Դ����ѺƷС��       
          '0' , --�Ƿ�Ϊ�չ��������в�����������е�ծȯ       
          ''  , --Ȩ�ط��ϸ��ʶ       
          ''  , --�����������ϸ��ʶ       
          ''  , --Ȩ�ط�����ѺƷ����       
          ''  , --Ȩ�ط�����ѺƷϸ��       
          ''  , --����������ѺƷ����       
          T1.BILL_AMT , --��Ѻ�ܶ�        
          NVL(T1.CCY_CD,'CNY') , --����        
          T1.ISSUE_DT , --��ʼ����        
          T1.MATU_DT  , --��������        
          CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.ISSUE_DT,'YYYY-MM-DD')) / 365<0
                                THEN 0
                                ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.ISSUE_DT,'YYYY-MM-DD')) / 365
                          END  , --ԭʼ����        
          CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                        THEN 0
                        ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                  END  , --ʣ������        
          '0' , --���й����ۿ�ϵ����ʶ        
          1 , --�ڲ��ۿ�ϵ��        
          ''  , --������ѺƷ����       
          '0' , --�ʲ�֤ȯ����ʶ       
          ''  , --������������        
          ''  , --������ѺƷ���еȼ�       
          '02'  , --������ѺƷ���������        
          --'CHN' , --������ѺƷ������ע�����        
          '01',--�й�
          CASE WHEN (TO_DATE(T1.DISC_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                        THEN 0
                        ELSE (TO_DATE(T1.DISC_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                  END  , --������ѺƷʣ������       
          1 , --�ع�Ƶ��        
          ''  , --������        
          ''    --�����˾���ע����ⲿ����        �ޣ�ĿǰƱ��ҵ���������ҵ��
    FROM	BRD_BILL			T1
    WHERE T1.ATL_PAY_AMT <> 0 --ȡ�Ķ��Ǳ���
            AND SUBSTR(T1.SBJT_CD, 1, 6) IN (
                '130101', --�����ʲ�-���гжһ�Ʊ����
                '130103', --�����ʲ�-���гжһ�Ʊת����
                 '130102' --��ҵ��Ʊ����         ת������ȡ���ж��У�����ҪƱ����Ϊ����
            )
            AND T1.DATANO=p_data_dt_str;
    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_PJ_COLLATERAL',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_PJ_COLLATERAL;

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := 'Ʊ��ת���ֵ���ѺƷ('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_PJ_COLLATERAL;
/

