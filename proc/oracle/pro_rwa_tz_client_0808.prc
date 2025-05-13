CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_TZ_CLIENT_0808(p_data_dt_str IN  VARCHAR2, --��������
                                              p_po_rtncode  OUT VARCHAR2, --���ر��
                                              p_po_rtnmsg   OUT VARCHAR2  --��������
)
  /*
    �洢��������:RWA_DEV.PRO_RWA_TZ_CLIENT_0808
    ʵ�ֹ���:����ϵͳ���������Ϣȫ������RWA�ӿڱ����������)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :QHJIANG
    ��дʱ��:2016-04-14
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.ZGS_ATBOND|ծȯ��Ϣ��
    Դ  ��2 :RWA_DEV.ZGS_ATINTRUST_PLAN|�ʲ�����ƻ���
    Դ  ��3 :RWA_DEV.ZGS_INVESTASSETDETAIL|������ϸ��
    Դ  ��4 :RWA_DEV.ZGS_FINANCING_INFO|��Ʒ��Ϣ��
    Ŀ���1 :RWA_DEV.RWA_TZ_CLIENT|RWA����������Ϣ��
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */

  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_TZ_CLIENT_0808';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;


  BEGIN
    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --���Ŀ����е�ԭ�м�¼
    --DELETE FROM RWA_DEV.RWA_TZ_CLIENT WHERE DATADATE=TO_DATE(p_data_dt_str,'yyyyMMdd');
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TZ_CLIENT';

    --�����ծȯͶ��-��ծ������Ĭ�ϣ�ǿ�Ʋ����ծ������Ϊ��������
    INSERT INTO RWA_DEV.RWA_TZ_CLIENT(
         DataDate                   --��������
        ,DataNo                     --������ˮ��
        ,ClientID                   --��������ID
        ,SourceClientID             --Դ��������ID
        ,SSysID                     --ԴϵͳID
        ,ClientName                 --������������
        ,SOrgID                     --Դ����ID
        ,SOrgName                   --Դ��������
        ,OrgSortNo                  --�������������
        ,OrgID                      --��������ID
        ,OrgName                    --������������
        ,IndustryID                 --������ҵ����
        ,IndustryName               --������ҵ����
        ,ClientType                 --�����������
        ,ClientSubType              --��������С��
        ,RegistState                --ע����һ����
        ,RCERating                  --����ע����ⲿ����
        ,RCERAgency                 --����ע����ⲿ��������
        ,OrganizationCode           --��֯��������
        ,ConsolidatedSCFlag         --�Ƿ񲢱��ӹ�˾
        ,SLClientFlag               --רҵ����ͻ���ʶ
        ,SLClientType               --רҵ����ͻ�����
        ,ExpoCategoryIRB            --��������¶���
        ,ModelID                    --ģ��ID
        ,ModelIRating               --ģ���ڲ�����
        ,ModelPD                    --ģ��ΥԼ����
        ,IRating                    --�ڲ�����
        ,PD                         --ΥԼ����
        ,DefaultFlag                --ΥԼ��ʶ
        ,NewDefaultFlag             --����ΥԼ��ʶ
        ,DefaultDate                --ΥԼʱ��
        ,ClientERating              --���������ⲿ����
        ,CCPFlag                    --���뽻�׶��ֱ�ʶ
        ,QualCCPFlag                --�Ƿ�ϸ����뽻�׶���
        ,ClearMemberFlag            --�����Ա��ʶ
        ,CompanySize                --��ҵ��ģ
        ,SSMBFlag                   --��׼С΢��ҵ��ʶ
        ,SSMBFLAGSTD         				--Ȩ�ط���׼С΢��ҵ��ʶ
        ,AnnualSale                 --��˾�ͻ������۶�
        ,CountryCode                --ע����Ҵ���
        ,MSMBFlag										--���Ų�΢С��ҵ��ʶ
    )
    SELECT
        				 TO_DATE(p_data_dt_str,'yyyyMMdd')     																	AS DataDate            		--��������
        				,p_data_dt_str                         																	AS DataNo              		--������ˮ��
        				,'ZGZYZF'																																AS ClientID            		--��������ID            Ĭ�� �й���������
        				,'ZGZYZF'																																AS SourceClientID      		--Դ��������ID
        				,'TZ'                                 																	AS SSysID              		--ԴϵͳID
        				,'�й���������'																													AS ClientName          		--������������          Ĭ�� �й���������
        				,'10000000'							              																	AS SOrgID              		--Դ����ID
        				,'��������'																															AS SOrgName            		--Դ��������
        				,'1'																																		AS OrgSortNo           		--�������������
        				,'10000000'								             																	AS OrgID               		--��������ID
        				,'��������'																															AS OrgName             		--������������
        				,'999999'														  																	AS IndustryID          		--������ҵ����          Ĭ�� 999999-δ֪
        				,'δ֪'											          																	AS IndustryName        		--������ҵ����          Ĭ�� 999999-δ֪
        				,'01'																																		AS ClientType          		--�����������          Ĭ�� 01-��Ȩ
        				,'0101'																																	AS ClientSubType       		--��������С��          Ĭ�� 0101-�й���������
        				,'01'	                                 																	AS RegistState         		--ע����һ����        Ĭ�� 01-����
        				,'0104'					                       																	AS RCERating           		--����ע����ⲿ����
        				,'01'                                  																	AS RCERAgency          		--����ע����ⲿ��������
        				,'ZGZYZFZZJGDM'						             																	AS OrganizationCode    		--��֯��������
        				,'0'	                                																	AS ConsolidatedSCFlag  		--�Ƿ񲢱��ӹ�˾
        				,'0'                                  																	AS SLClientFlag        		--רҵ����ͻ���ʶ
        				,''	                                  																	AS SLClientType        		--רҵ����ͻ�����
        				,'020101'                              																	AS ExpoCategoryIRB     		--��������¶���        Ĭ�� 020101-��������
        				,''				                            																	AS ModelID             		--ģ��ID
        				,''				                            																	AS ModelIRating        		--ģ���ڲ�����
        				,NULL                                 																	AS ModelPD             		--ģ��ΥԼ����
        				,''								                    																	AS IRating             		--�ڲ�����
        				,NULL	                                																	AS PD                  		--ΥԼ����
        				,'0'																																		AS DefaultFlag         		--ΥԼ��ʶ
        				,'0'											            																	AS NewDefaultFlag      		--����ΥԼ��ʶ
        				,''																     																	AS DefaultDate         		--ΥԼʱ��
        				,'0104'														     																	AS ClientERating       		--���������ⲿ����
        				,'0'                                  																	AS CCPFlag             		--���뽻�׶��ֱ�ʶ
        				,'0'                                   																	AS QualCCPFlag         		--�Ƿ�ϸ����뽻�׶���
        				,'0'                                   																	AS ClearMemberFlag     		--�����Ա��ʶ
        				,'00'									                																	AS CompanySize         		--��ҵ��ģ
        				,'0'											            																	AS SSMBFlag            		--��׼С΢��ҵ��ʶ
        				,'0'											            																	AS SSMBFlagSTD         		--Ȩ�ط���׼С΢��ҵ��ʶ
        				,NULL							                     																	AS AnnualSale          		--��˾�ͻ������۶�
        				,'CHN'																																	AS CountryCode            --ע����Ҵ���
        				,''																																			AS MSMBFlag								--���Ų�΢С��ҵ��ʶ

    FROM				DUAL
	  ;

		COMMIT;

		--Ӧ�տ�Ͷ��-ʵ��������Ĭ�ϣ�ǿ�Ʋ���ʵ��������Ϊһ�㹫˾
		INSERT INTO RWA_DEV.RWA_TZ_CLIENT(
         DataDate                   --��������
        ,DataNo                     --������ˮ��
        ,ClientID                   --��������ID
        ,SourceClientID             --Դ��������ID
        ,SSysID                     --ԴϵͳID
        ,ClientName                 --������������
        ,SOrgID                     --Դ����ID
        ,SOrgName                   --Դ��������
        ,OrgSortNo                  --�������������
        ,OrgID                      --��������ID
        ,OrgName                    --������������
        ,IndustryID                 --������ҵ����
        ,IndustryName               --������ҵ����
        ,ClientType                 --�����������
        ,ClientSubType              --��������С��
        ,RegistState                --ע����һ����
        ,RCERating                  --����ע����ⲿ����
        ,RCERAgency                 --����ע����ⲿ��������
        ,OrganizationCode           --��֯��������
        ,ConsolidatedSCFlag         --�Ƿ񲢱��ӹ�˾
        ,SLClientFlag               --רҵ����ͻ���ʶ
        ,SLClientType               --רҵ����ͻ�����
        ,ExpoCategoryIRB            --��������¶���
        ,ModelID                    --ģ��ID
        ,ModelIRating               --ģ���ڲ�����
        ,ModelPD                    --ģ��ΥԼ����
        ,IRating                    --�ڲ�����
        ,PD                         --ΥԼ����
        ,DefaultFlag                --ΥԼ��ʶ
        ,NewDefaultFlag             --����ΥԼ��ʶ
        ,DefaultDate                --ΥԼʱ��
        ,ClientERating              --���������ⲿ����
        ,CCPFlag                    --���뽻�׶��ֱ�ʶ
        ,QualCCPFlag                --�Ƿ�ϸ����뽻�׶���
        ,ClearMemberFlag            --�����Ա��ʶ
        ,CompanySize                --��ҵ��ģ
        ,SSMBFlag                   --��׼С΢��ҵ��ʶ
        ,SSMBFLAGSTD         				--Ȩ�ط���׼С΢��ҵ��ʶ
        ,AnnualSale                 --��˾�ͻ������۶�
        ,CountryCode                --ע����Ҵ���
        ,MSMBFlag										--���Ų�΢С��ҵ��ʶ
    )
    SELECT
        				 TO_DATE(p_data_dt_str,'yyyyMMdd')     																	AS DataDate            		--��������
        				,p_data_dt_str                         																	AS DataNo              		--������ˮ��
        				,'XN-YBGS'																															AS ClientID            		--��������ID            Ĭ�� �й���������
        				,'XN-YBGS'																															AS SourceClientID      		--Դ��������ID
        				,'TZ'                                 																	AS SSysID              		--ԴϵͳID
        				,'����һ�㹫˾'																													AS ClientName          		--������������          Ĭ�� �й���������
        				,'10000000'							              																	AS SOrgID              		--Դ����ID
        				,'��������'																															AS SOrgName            		--Դ��������
        				,'1'																																		AS OrgSortNo           		--�������������
        				,'10000000'								             																	AS OrgID               		--��������ID
        				,'��������'																															AS OrgName             		--������������
        				,'999999'														  																	AS IndustryID          		--������ҵ����          Ĭ�� 999999-δ֪
        				,'δ֪'											          																	AS IndustryName        		--������ҵ����          Ĭ�� 999999-δ֪
        				,'03'																																		AS ClientType          		--�����������          Ĭ�� 03-��˾
        				,'0301'																																	AS ClientSubType       		--��������С��          Ĭ�� 0301-һ�㹫˾
        				,'01'	                                 																	AS RegistState         		--ע����һ����        Ĭ�� 01-����
        				,'0104'					                       																	AS RCERating           		--����ע����ⲿ����
        				,'01'                                  																	AS RCERAgency          		--����ע����ⲿ��������
        				,'XNYBGSZZJGDM'						             																	AS OrganizationCode    		--��֯��������
        				,'0'	                                																	AS ConsolidatedSCFlag  		--�Ƿ񲢱��ӹ�˾
        				,'0'                                  																	AS SLClientFlag        		--רҵ����ͻ���ʶ
        				,''	                                  																	AS SLClientType        		--רҵ����ͻ�����
        				,'020301'                              																	AS ExpoCategoryIRB     		--��������¶���        Ĭ�� 020301-һ�㹫˾
        				,''				                            																	AS ModelID             		--ģ��ID
        				,''				                            																	AS ModelIRating        		--ģ���ڲ�����
        				,NULL                                 																	AS ModelPD             		--ģ��ΥԼ����
        				,''								                    																	AS IRating             		--�ڲ�����
        				,NULL	                                																	AS PD                  		--ΥԼ����
        				,'0'																																		AS DefaultFlag         		--ΥԼ��ʶ
        				,'0'											            																	AS NewDefaultFlag      		--����ΥԼ��ʶ
        				,''																     																	AS DefaultDate         		--ΥԼʱ��
        				,'0104'														     																	AS ClientERating       		--���������ⲿ����
        				,'0'                                  																	AS CCPFlag             		--���뽻�׶��ֱ�ʶ
        				,'0'                                   																	AS QualCCPFlag         		--�Ƿ�ϸ����뽻�׶���
        				,'0'                                   																	AS ClearMemberFlag     		--�����Ա��ʶ
        				,'00'									                																	AS CompanySize         		--��ҵ��ģ
        				,'0'											            																	AS SSMBFlag            		--��׼С΢��ҵ��ʶ
        				,'0'											            																	AS SSMBFlagSTD         		--Ȩ�ط���׼С΢��ҵ��ʶ
        				,NULL							                     																	AS AnnualSale          		--��˾�ͻ������۶�
        				,'CHN'																																	AS CountryCode            --ע����Ҵ���
        				,''																																			AS MSMBFlag								--���Ų�΢С��ҵ��ʶ

    FROM				DUAL
	  ;

		COMMIT;

		--Ʊ��(ת)����-���׶���Ĭ�ϣ�ǿ�Ʋ��뽻�׶���Ϊ�й���ҵ����
		INSERT INTO RWA_DEV.RWA_TZ_CLIENT(
         DataDate                   --��������
        ,DataNo                     --������ˮ��
        ,ClientID                   --��������ID
        ,SourceClientID             --Դ��������ID
        ,SSysID                     --ԴϵͳID
        ,ClientName                 --������������
        ,SOrgID                     --Դ����ID
        ,SOrgName                   --Դ��������
        ,OrgSortNo                  --�������������
        ,OrgID                      --��������ID
        ,OrgName                    --������������
        ,IndustryID                 --������ҵ����
        ,IndustryName               --������ҵ����
        ,ClientType                 --�����������
        ,ClientSubType              --��������С��
        ,RegistState                --ע����һ����
        ,RCERating                  --����ע����ⲿ����
        ,RCERAgency                 --����ע����ⲿ��������
        ,OrganizationCode           --��֯��������
        ,ConsolidatedSCFlag         --�Ƿ񲢱��ӹ�˾
        ,SLClientFlag               --רҵ����ͻ���ʶ
        ,SLClientType               --רҵ����ͻ�����
        ,ExpoCategoryIRB            --��������¶���
        ,ModelID                    --ģ��ID
        ,ModelIRating               --ģ���ڲ�����
        ,ModelPD                    --ģ��ΥԼ����
        ,IRating                    --�ڲ�����
        ,PD                         --ΥԼ����
        ,DefaultFlag                --ΥԼ��ʶ
        ,NewDefaultFlag             --����ΥԼ��ʶ
        ,DefaultDate                --ΥԼʱ��
        ,ClientERating              --���������ⲿ����
        ,CCPFlag                    --���뽻�׶��ֱ�ʶ
        ,QualCCPFlag                --�Ƿ�ϸ����뽻�׶���
        ,ClearMemberFlag            --�����Ա��ʶ
        ,CompanySize                --��ҵ��ģ
        ,SSMBFlag                   --��׼С΢��ҵ��ʶ
        ,SSMBFLAGSTD         				--Ȩ�ط���׼С΢��ҵ��ʶ
        ,AnnualSale                 --��˾�ͻ������۶�
        ,CountryCode                --ע����Ҵ���
        ,MSMBFlag										--���Ų�΢С��ҵ��ʶ
    )
    SELECT
        				 TO_DATE(p_data_dt_str,'yyyyMMdd')     																	AS DataDate            		--��������
        				,p_data_dt_str                         																	AS DataNo              		--������ˮ��
        				,'XN-ZGSYYH'																														AS ClientID            		--��������ID            Ĭ�� �й���������
        				,'XN-ZGSYYH'																														AS SourceClientID      		--Դ��������ID
        				,'TZ'                                 																	AS SSysID              		--ԴϵͳID
        				,'�����й���ҵ����'																											AS ClientName          		--������������          Ĭ�� �й���������
        				,'10000000'							              																	AS SOrgID              		--Դ����ID
        				,'��������'																															AS SOrgName            		--Դ��������
        				,'1'																																		AS OrgSortNo           		--�������������
        				,'10000000'								             																	AS OrgID               		--��������ID
        				,'��������'																															AS OrgName             		--������������
        				,'J6620'														  																	AS IndustryID          		--������ҵ����          Ĭ�� J6620-�������з���
        				,'�������з���'							          																	AS IndustryName        		--������ҵ����          Ĭ�� J6620-�������з���
        				,'02'																																		AS ClientType          		--�����������          Ĭ�� 02-���ڻ���
        				,'0202'																																	AS ClientSubType       		--��������С��          Ĭ�� 0202-�й���ҵ����
        				,'01'	                                 																	AS RegistState         		--ע����һ����        Ĭ�� 01-����
        				,'0104'					                       																	AS RCERating           		--����ע����ⲿ����
        				,'01'                                  																	AS RCERAgency          		--����ע����ⲿ��������
        				,'XNZGSYYHZZJGDM'					             																	AS OrganizationCode    		--��֯��������
        				,'0'	                                																	AS ConsolidatedSCFlag  		--�Ƿ񲢱��ӹ�˾
        				,'0'                                  																	AS SLClientFlag        		--רҵ����ͻ���ʶ
        				,''	                                  																	AS SLClientType        		--רҵ����ͻ�����
        				,'020201'                              																	AS ExpoCategoryIRB     		--��������¶���        Ĭ�� 020201-��������ڻ���
        				,''				                            																	AS ModelID             		--ģ��ID
        				,''				                            																	AS ModelIRating        		--ģ���ڲ�����
        				,NULL                                 																	AS ModelPD             		--ģ��ΥԼ����
        				,''								                    																	AS IRating             		--�ڲ�����
        				,NULL	                                																	AS PD                  		--ΥԼ����
        				,'0'																																		AS DefaultFlag         		--ΥԼ��ʶ
        				,'0'											            																	AS NewDefaultFlag      		--����ΥԼ��ʶ
        				,''																     																	AS DefaultDate         		--ΥԼʱ��
        				,'0104'														     																	AS ClientERating       		--���������ⲿ����
        				,'0'                                  																	AS CCPFlag             		--���뽻�׶��ֱ�ʶ
        				,'0'                                   																	AS QualCCPFlag         		--�Ƿ�ϸ����뽻�׶���
        				,'0'                                   																	AS ClearMemberFlag     		--�����Ա��ʶ
        				,'00'									                																	AS CompanySize         		--��ҵ��ģ
        				,'0'											            																	AS SSMBFlag            		--��׼С΢��ҵ��ʶ
        				,'0'											            																	AS SSMBFlagSTD         		--Ȩ�ط���׼С΢��ҵ��ʶ
        				,NULL							                     																	AS AnnualSale          		--��˾�ͻ������۶�
        				,'CHN'																																	AS CountryCode            --ע����Ҵ���
        				,''																																			AS MSMBFlag								--���Ų�΢С��ҵ��ʶ

    FROM				DUAL
	  ;

		COMMIT;

		--�Ŵ�-���׶���Ĭ�ϣ�ǿ�Ʋ��뽻�׶���Ϊ����
		INSERT INTO RWA_DEV.RWA_TZ_CLIENT(
         DataDate                   --��������
        ,DataNo                     --������ˮ��
        ,ClientID                   --��������ID
        ,SourceClientID             --Դ��������ID
        ,SSysID                     --ԴϵͳID
        ,ClientName                 --������������
        ,SOrgID                     --Դ����ID
        ,SOrgName                   --Դ��������
        ,OrgSortNo                  --�������������
        ,OrgID                      --��������ID
        ,OrgName                    --������������
        ,IndustryID                 --������ҵ����
        ,IndustryName               --������ҵ����
        ,ClientType                 --�����������
        ,ClientSubType              --��������С��
        ,RegistState                --ע����һ����
        ,RCERating                  --����ע����ⲿ����
        ,RCERAgency                 --����ע����ⲿ��������
        ,OrganizationCode           --��֯��������
        ,ConsolidatedSCFlag         --�Ƿ񲢱��ӹ�˾
        ,SLClientFlag               --רҵ����ͻ���ʶ
        ,SLClientType               --רҵ����ͻ�����
        ,ExpoCategoryIRB            --��������¶���
        ,ModelID                    --ģ��ID
        ,ModelIRating               --ģ���ڲ�����
        ,ModelPD                    --ģ��ΥԼ����
        ,IRating                    --�ڲ�����
        ,PD                         --ΥԼ����
        ,DefaultFlag                --ΥԼ��ʶ
        ,NewDefaultFlag             --����ΥԼ��ʶ
        ,DefaultDate                --ΥԼʱ��
        ,ClientERating              --���������ⲿ����
        ,CCPFlag                    --���뽻�׶��ֱ�ʶ
        ,QualCCPFlag                --�Ƿ�ϸ����뽻�׶���
        ,ClearMemberFlag            --�����Ա��ʶ
        ,CompanySize                --��ҵ��ģ
        ,SSMBFlag                   --��׼С΢��ҵ��ʶ
        ,SSMBFLAGSTD         				--Ȩ�ط���׼С΢��ҵ��ʶ
        ,AnnualSale                 --��˾�ͻ������۶�
        ,CountryCode                --ע����Ҵ���
        ,MSMBFlag										--���Ų�΢С��ҵ��ʶ
    )
    SELECT
        				 TO_DATE(p_data_dt_str,'yyyyMMdd')     																	AS DataDate            		--��������
        				,p_data_dt_str                         																	AS DataNo              		--������ˮ��
        				,'XN-GRKH'																															AS ClientID            		--��������ID            Ĭ�� ���˿ͻ�
        				,'XN-GRKH'																															AS SourceClientID      		--Դ��������ID
        				,'TZ'                                 																	AS SSysID              		--ԴϵͳID
        				,'������˿ͻ�'																													AS ClientName          		--������������          Ĭ�� ���˿ͻ�
        				,'10000000'							              																	AS SOrgID              		--Դ����ID
        				,'��������'																															AS SOrgName            		--Դ��������
        				,'1'																																		AS OrgSortNo           		--�������������
        				,'10000000'								             																	AS OrgID               		--��������ID
        				,'��������'																															AS OrgName             		--������������
        				,''																	  																	AS IndustryID          		--������ҵ����          Ĭ�� ��
        				,''													          																	AS IndustryName        		--������ҵ����          Ĭ�� ��
        				,'04'																																		AS ClientType          		--�����������          Ĭ�� 04-����
        				,'0401'																																	AS ClientSubType       		--��������С��          Ĭ�� 0401-���ˣ���Ȼ�ˣ�
        				,'01'	                                 																	AS RegistState         		--ע����һ����        Ĭ�� 01-����
        				,''							                       																	AS RCERating           		--����ע����ⲿ����    Ĭ�� ��
        				,''	                                  																	AS RCERAgency          		--����ע����ⲿ�������� Ĭ�� ��
        				,''												             																	AS OrganizationCode    		--��֯��������          Ĭ�� ��
        				,'0'	                                																	AS ConsolidatedSCFlag  		--�Ƿ񲢱��ӹ�˾        Ĭ�� ��
        				,'0'                                  																	AS SLClientFlag        		--רҵ����ͻ���ʶ      Ĭ�� 0-��
        				,''	                                  																	AS SLClientType        		--רҵ����ͻ�����      Ĭ�� 0-��
        				,'020403'                              																	AS ExpoCategoryIRB     		--��������¶���        Ĭ�� 020403-��������
        				,''				                            																	AS ModelID             		--ģ��ID
        				,''				                            																	AS ModelIRating        		--ģ���ڲ�����
        				,NULL                                 																	AS ModelPD             		--ģ��ΥԼ����
        				,''								                    																	AS IRating             		--�ڲ�����
        				,NULL	                                																	AS PD                  		--ΥԼ����
        				,'0'																																		AS DefaultFlag         		--ΥԼ��ʶ
        				,'0'											            																	AS NewDefaultFlag      		--����ΥԼ��ʶ
        				,''																     																	AS DefaultDate         		--ΥԼʱ��
        				,''																     																	AS ClientERating       		--���������ⲿ����
        				,'0'                                  																	AS CCPFlag             		--���뽻�׶��ֱ�ʶ
        				,'0'                                   																	AS QualCCPFlag         		--�Ƿ�ϸ����뽻�׶���
        				,'0'                                   																	AS ClearMemberFlag     		--�����Ա��ʶ
        				,'00'									                																	AS CompanySize         		--��ҵ��ģ
        				,'0'											            																	AS SSMBFlag            		--��׼С΢��ҵ��ʶ
        				,'0'											            																	AS SSMBFlagSTD         		--Ȩ�ط���׼С΢��ҵ��ʶ
        				,NULL							                     																	AS AnnualSale          		--��˾�ͻ������۶�
        				,'CHN'																																	AS CountryCode            --ע����Ҵ���
        				,''																																			AS MSMBFlag								--���Ų�΢С��ҵ��ʶ

    FROM				DUAL
	  ;

		COMMIT;

    --���ծȯͶ��-��Ȩ�෢����Ĭ��
    INSERT INTO RWA_DEV.RWA_TZ_CLIENT(
         DataDate                   --��������
        ,DataNo                     --������ˮ��
        ,ClientID                   --��������ID
        ,SourceClientID             --Դ��������ID
        ,SSysID                     --ԴϵͳID
        ,ClientName                 --������������
        ,SOrgID                     --Դ����ID
        ,SOrgName                   --Դ��������
        ,OrgSortNo                  --�������������
        ,OrgID                      --��������ID
        ,OrgName                    --������������
        ,IndustryID                 --������ҵ����
        ,IndustryName               --������ҵ����
        ,ClientType                 --�����������
        ,ClientSubType              --��������С��
        ,RegistState                --ע����һ����
        ,RCERating                  --����ע����ⲿ����
        ,RCERAgency                 --����ע����ⲿ��������
        ,OrganizationCode           --��֯��������
        ,ConsolidatedSCFlag         --�Ƿ񲢱��ӹ�˾
        ,SLClientFlag               --רҵ����ͻ���ʶ
        ,SLClientType               --רҵ����ͻ�����
        ,ExpoCategoryIRB            --��������¶���
        ,ModelID                    --ģ��ID
        ,ModelIRating               --ģ���ڲ�����
        ,ModelPD                    --ģ��ΥԼ����
        ,IRating                    --�ڲ�����
        ,PD                         --ΥԼ����
        ,DefaultFlag                --ΥԼ��ʶ
        ,NewDefaultFlag             --����ΥԼ��ʶ
        ,DefaultDate                --ΥԼʱ��
        ,ClientERating              --���������ⲿ����
        ,CCPFlag                    --���뽻�׶��ֱ�ʶ
        ,QualCCPFlag                --�Ƿ�ϸ����뽻�׶���
        ,ClearMemberFlag            --�����Ա��ʶ
        ,CompanySize                --��ҵ��ģ
        ,SSMBFlag                   --��׼С΢��ҵ��ʶ
        ,SSMBFLAGSTD         				--Ȩ�ط���׼С΢��ҵ��ʶ
        ,AnnualSale                 --��˾�ͻ������۶�
        ,CountryCode                --ע����Ҵ���
        ,MSMBFlag										--���Ų�΢С��ҵ��ʶ
    )
  	WITH TEMP_BND_BOOK AS (
    										SELECT BOND_ID,
												       INITIAL_COST,
												       INT_ADJUST,
												       MKT_VALUE_CHANGE,
												       RECEIVABLE_INT,
												       ACCOUNTABLE_INT
												  FROM (SELECT BOND_ID,
												               INITIAL_COST,
												               INT_ADJUST,
												               MKT_VALUE_CHANGE,
												               RECEIVABLE_INT,
												               ACCOUNTABLE_INT,
												               ROW_NUMBER() OVER(PARTITION BY BOND_ID ORDER BY SORT_SEQ DESC) RM
												          FROM FNS_BND_BOOK_B
												         WHERE AS_OF_DATE <= p_data_dt_str
												           AND DATANO = p_data_dt_str)
												 WHERE RM = 1
												   AND NVL(INITIAL_COST, 0) + NVL(INT_ADJUST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0
		)
		, TMP_BND_ORG AS (
									SELECT T4.BONDPUBLISHCOUNTRY		AS COUNTRYCODE
												,T4.MARKETSCATEGORY				AS MARKETSCATEGORY
												,MIN(T1.DEPARTMENT) 			AS ORGID
										FROM RWA_DEV.FNS_BND_INFO_B T1
							INNER JOIN TEMP_BND_BOOK T2
											ON T1.BOND_ID = T2.BOND_ID
							INNER JOIN RWA_DEV.NCM_BUSINESS_DUEBILL T3														--�Ŵ���ݱ�
											ON 'CW_IMPORTDATA' || T1.BOND_ID = T3.THIRDPARTYACCOUNTS
										 AND T3.BUSINESSTYPE = '1040202011'															--���ծȯͶ��
										 AND T3.DATANO = p_data_dt_str
							INNER JOIN RWA_DEV.NCM_BOND_INFO T4																		--�Ŵ�ծȯ��Ϣ��
	  									ON T3.RELATIVESERIALNO2 = T4.OBJECTNO
										 AND T4.OBJECTTYPE = 'BusinessContract'
										 AND T4.BONDFLAG04 = '1'																				--ծȯ����������Ȩ��
										 AND T4.DATANO = p_data_dt_str
	  							 WHERE (T1.ASSET_CLASS = '20' OR
								 					(T1.ASSET_CLASS = '40' AND T1.BOND_TYPE1 NOT IN ('091','099')) OR
								 					(T1.ASSET_CLASS = '40' AND T1.BOND_TYPE1 IN ('091','099') AND T1.CLOSED = '1')
												 )
	  																																--ͨ���ʲ�������ȷ��ծȯ����Ӧ�տ�Ͷ�ʡ�
	  																																--40 �ɹ��������ʲ�
														--����Ϲ����ݼ���ծȯͶ�ʷ�Χ����Ҫ�ų���ծȯ���ࡱ=���ʲ�֧��֤ȯ���ļ�¼����Ϊ��һ�൥��ͨ����¼ģ���ȡ���ݡ���Ҫ������
										 AND T1.BOND_TYPE1 <> '060'
										 AND T1.BOND_ID NOT IN
													(SELECT ZQNM FROM RWA.RWA_WSIB_ABS_ISSUE_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
								 						UNION ALL
								 					SELECT ZQNM FROM RWA.RWA_WSIB_ABS_INVEST_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
													)
																																										--�����ñ��ų��ʲ�֤ȯ����ծȯ����
										 AND T1.DATANO = p_data_dt_str																	--ծȯ��Ϣ��,��ȡ��Ч��ծȯ��Ϣ
								GROUP BY T4.BONDPUBLISHCOUNTRY, T4.MARKETSCATEGORY
		)
    SELECT 			--DISTINCT
        				 TO_DATE(p_data_dt_str,'yyyyMMdd')     																	AS DataDate            		--��������
        				,p_data_dt_str                         																	AS DataNo              		--������ˮ��
        				,CASE WHEN T1.MARKETSCATEGORY = '01' THEN T1.COUNTRYCODE || 'ZYZF'																--������Ȩ���һ򾭼�ʵ���������������
                			WHEN T1.MARKETSCATEGORY = '02' THEN T1.COUNTRYCODE || 'ZYYH'																--������������
                			ELSE T1.COUNTRYCODE || 'BMST'																																--������һ����ע��Ĺ�������ʵ��
                 END																																		AS ClientID            		--��������ID            Ĭ�� �й���������
        				,CASE WHEN T1.MARKETSCATEGORY = '01' THEN T1.COUNTRYCODE || 'ZYZF'
                			WHEN T1.MARKETSCATEGORY = '02' THEN T1.COUNTRYCODE || 'ZYYH'
                			ELSE T1.COUNTRYCODE || 'BMST'
                 END																																		AS SourceClientID      		--Դ��������ID
        				,'TZ'                                 																	AS SSysID              		--ԴϵͳID
        				,CASE WHEN T1.MARKETSCATEGORY = '01' THEN T1.COUNTRYCODE || '��������'														--������Ȩ���һ򾭼�ʵ���������������
                			WHEN T1.MARKETSCATEGORY = '02' THEN T1.COUNTRYCODE || '��������'														--������������
                			ELSE T1.COUNTRYCODE || '��������ʵ��'																												--������һ����ע��Ĺ�������ʵ��
                 END																																		AS ClientName          		--������������          Ĭ�� �й���������
        				,T1.ORGID								              																	AS SOrgID              		--Դ����ID
        				,T2.ORGNAME																															AS SOrgName            		--Դ��������
        				,T2.SORTNO																															AS OrgSortNo           		--�������������
        				,T1.ORGID									             																	AS OrgID               		--��������ID
        				,T2.ORGNAME																															AS OrgName             		--������������
        				,'999999'														  																	AS IndustryID          		--������ҵ����          Ĭ�� 999999-δ֪
        				,'δ֪'											          																	AS IndustryName        		--������ҵ����          Ĭ�� 999999-δ֪
        				,'01'																																		AS ClientType          		--�����������          Ĭ�� 01-��Ȩ
        				,CASE WHEN T1.MARKETSCATEGORY = '01' THEN '0102'																									--������Ȩ���һ򾭼�ʵ���������������
                			WHEN T1.MARKETSCATEGORY = '02' THEN '0104'																									--������������
                			ELSE '0107'																																									--������һ����ע��Ĺ�������ʵ��
                 END																																		AS ClientSubType       		--��������С��          Ĭ�� 0101-�й���������
        				,'02'	                                 																	AS RegistState         		--ע����һ����        Ĭ�� 02-����
        				,T3.RATINGRESULT                       																	AS RCERating           		--����ע����ⲿ����
        				,'01'                                  																	AS RCERAgency          		--����ע����ⲿ��������
        				,T1.COUNTRYCODE || 'ZZJGDM'				     																	AS OrganizationCode    		--��֯��������
        				,'0'	                                																	AS ConsolidatedSCFlag  		--�Ƿ񲢱��ӹ�˾
        				,'0'                                  																	AS SLClientFlag        		--רҵ����ͻ���ʶ
        				,''	                                  																	AS SLClientType        		--רҵ����ͻ�����
        				,CASE WHEN T1.MARKETSCATEGORY = '01' THEN '020101'																								--������Ȩ���һ򾭼�ʵ���������������
                			WHEN T1.MARKETSCATEGORY = '02' THEN '020102'																								--������������
                			ELSE '020103'																																								--������һ����ע��Ĺ�������ʵ��
                 END		                              																	AS ExpoCategoryIRB     		--��������¶���        Ĭ�� 020101-��������
        				,''				                            																	AS ModelID             		--ģ��ID
        				,''				                            																	AS ModelIRating        		--ģ���ڲ�����
        				,NULL                                 																	AS ModelPD             		--ģ��ΥԼ����
        				,''								                    																	AS IRating             		--�ڲ�����
        				,NULL	                                																	AS PD                  		--ΥԼ����
        				,'0'																																		AS DefaultFlag         		--ΥԼ��ʶ
        				,'0'											            																	AS NewDefaultFlag      		--����ΥԼ��ʶ
        				,''																     																	AS DefaultDate         		--ΥԼʱ��
        				,T3.RATINGRESULT									     																	AS ClientERating       		--���������ⲿ����
        				,'0'                                  																	AS CCPFlag             		--���뽻�׶��ֱ�ʶ
        				,'0'                                   																	AS QualCCPFlag         		--�Ƿ�ϸ����뽻�׶���
        				,'0'                                   																	AS ClearMemberFlag     		--�����Ա��ʶ
        				,'00'									                																	AS CompanySize         		--��ҵ��ģ
        				,'0'											            																	AS SSMBFlag            		--��׼С΢��ҵ��ʶ
        				,'0'											            																	AS SSMBFlagSTD         		--Ȩ�ط���׼С΢��ҵ��ʶ
        				,NULL							                     																	AS AnnualSale          		--��˾�ͻ������۶�
        				,T1.COUNTRYCODE																													AS CountryCode            --ע����Ҵ���
        				,''																																			AS MSMBFlag								--���Ų�΢С��ҵ��ʶ

    FROM				TMP_BND_ORG T1
    LEFT JOIN		RWA.ORG_INFO T2
    ON					T1.ORGID = T2.ORGID
    LEFT JOIN		RWA.RWA_WP_COUNTRYRATING T3
    ON					T1.COUNTRYCODE = T3.COUNTRYCODE
    AND					T3.ISINUSE = '1'
	  ;

		COMMIT;

		--ë��ϯ����
    INSERT INTO RWA_DEV.RWA_TZ_CLIENT(
         DataDate                   --��������
        ,DataNo                     --������ˮ��
        ,ClientID                   --��������ID
        ,SourceClientID             --Դ��������ID
        ,SSysID                     --ԴϵͳID
        ,ClientName                 --������������
        ,SOrgID                     --Դ����ID
        ,SOrgName                   --Դ��������
        ,OrgSortNo                  --�������������
        ,OrgID                      --��������ID
        ,OrgName                    --������������
        ,IndustryID                 --������ҵ����
        ,IndustryName               --������ҵ����
        ,ClientType                 --�����������
        ,ClientSubType              --��������С��
        ,RegistState                --ע����һ����
        ,RCERating                  --����ע����ⲿ����
        ,RCERAgency                 --����ע����ⲿ��������
        ,OrganizationCode           --��֯��������
        ,ConsolidatedSCFlag         --�Ƿ񲢱��ӹ�˾
        ,SLClientFlag               --רҵ����ͻ���ʶ
        ,SLClientType               --רҵ����ͻ�����
        ,ExpoCategoryIRB            --��������¶���
        ,ModelID                    --ģ��ID
        ,ModelIRating               --ģ���ڲ�����
        ,ModelPD                    --ģ��ΥԼ����
        ,IRating                    --�ڲ�����
        ,PD                         --ΥԼ����
        ,DefaultFlag                --ΥԼ��ʶ
        ,NewDefaultFlag             --����ΥԼ��ʶ
        ,DefaultDate                --ΥԼʱ��
        ,ClientERating              --���������ⲿ����
        ,CCPFlag                    --���뽻�׶��ֱ�ʶ
        ,QualCCPFlag                --�Ƿ�ϸ����뽻�׶���
        ,ClearMemberFlag            --�����Ա��ʶ
        ,CompanySize                --��ҵ��ģ
        ,SSMBFlag                   --��׼С΢��ҵ��ʶ
        ,SSMBFLAGSTD         				--Ȩ�ط���׼С΢��ҵ��ʶ
        ,AnnualSale                 --��˾�ͻ������۶�
        ,CountryCode                --ע����Ҵ���
        ,MSMBFlag										--���Ų�΢С��ҵ��ʶ
    )
  	WITH TEMP_BND_BOOK AS (
    										SELECT BOND_ID,
												       INITIAL_COST,
												       INT_ADJUST,
												       MKT_VALUE_CHANGE,
												       RECEIVABLE_INT,
												       ACCOUNTABLE_INT
												  FROM (SELECT BOND_ID,
												               INITIAL_COST,
												               INT_ADJUST,
												               MKT_VALUE_CHANGE,
												               RECEIVABLE_INT,
												               ACCOUNTABLE_INT,
												               ROW_NUMBER() OVER(PARTITION BY BOND_ID ORDER BY SORT_SEQ DESC) RM
												          FROM FNS_BND_BOOK_B
												         WHERE AS_OF_DATE <= p_data_dt_str
												         	 AND BOND_ID = 'B200801010095'
												           AND DATANO = p_data_dt_str)
												 WHERE RM = 1
												   AND NVL(INITIAL_COST, 0) + NVL(INT_ADJUST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0
		)
		SELECT 			--DISTINCT
        				 TO_DATE(p_data_dt_str,'yyyyMMdd')     																	AS DataDate            		--��������
        				,p_data_dt_str                         																	AS DataNo              		--������ˮ��
        				,'MZXXZ'																																AS ClientID            		--��������ID            Ĭ�� MZXXZ
        				,'MZXXZ'																																AS SourceClientID      		--Դ��������ID
        				,'TZ'                                 																	AS SSysID              		--ԴϵͳID
        				,'ë��ϯ��'																															AS ClientName          		--������������          Ĭ�� ë��ϯ��
        				,T3.ORGID								              																	AS SOrgID              		--Դ����ID
        				,T3.ORGNAME																															AS SOrgName            		--Դ��������
        				,T3.SORTNO																															AS OrgSortNo           		--�������������
        				,T3.ORGID									             																	AS OrgID               		--��������ID
        				,T3.ORGNAME																															AS OrgName             		--������������
        				,'J66'															  																	AS IndustryID          		--������ҵ����          Ĭ�� J66-���ҽ��ڷ���
        				,'���ҽ��ڷ���'							          																	AS IndustryName        		--������ҵ����          Ĭ�� J66-���ҽ��ڷ���
        				,'02'																																		AS ClientType          		--�����������          Ĭ�� 02-���ڻ���
        				,'0205'																																	AS ClientSubType       		--��������С��          Ĭ�� 0205-�й��������ڻ���
        				,'01'	                                 																	AS RegistState         		--ע����һ����        Ĭ�� 01-����
        				,''							                       																	AS RCERating           		--����ע����ⲿ����
        				,'01'                                  																	AS RCERAgency          		--����ע����ⲿ��������
        				,'MZXXZZZJGDM'										     																	AS OrganizationCode    		--��֯��������
        				,'0'	                                																	AS ConsolidatedSCFlag  		--�Ƿ񲢱��ӹ�˾
        				,'0'                                  																	AS SLClientFlag        		--רҵ����ͻ���ʶ
        				,''	                                  																	AS SLClientType        		--רҵ����ͻ�����
        				,'020202'                              																	AS ExpoCategoryIRB     		--��������¶���        Ĭ�� 020202-����������ڻ���
        				,''				                            																	AS ModelID             		--ģ��ID
        				,''				                            																	AS ModelIRating        		--ģ���ڲ�����
        				,NULL                                 																	AS ModelPD             		--ģ��ΥԼ����
        				,''								                    																	AS IRating             		--�ڲ�����
        				,NULL	                                																	AS PD                  		--ΥԼ����
        				,'0'																																		AS DefaultFlag         		--ΥԼ��ʶ
        				,'0'											            																	AS NewDefaultFlag      		--����ΥԼ��ʶ
        				,''																     																	AS DefaultDate         		--ΥԼʱ��
        				,''																     																	AS ClientERating       		--���������ⲿ����
        				,'0'                                  																	AS CCPFlag             		--���뽻�׶��ֱ�ʶ
        				,'0'                                   																	AS QualCCPFlag         		--�Ƿ�ϸ����뽻�׶���
        				,'0'                                   																	AS ClearMemberFlag     		--�����Ա��ʶ
        				,'00'									                																	AS CompanySize         		--��ҵ��ģ
        				,'0'											            																	AS SSMBFlag            		--��׼С΢��ҵ��ʶ
        				,'0'											            																	AS SSMBFlagSTD         		--Ȩ�ط���׼С΢��ҵ��ʶ
        				,NULL							                     																	AS AnnualSale          		--��˾�ͻ������۶�
        				,'CHN'																																	AS CountryCode            --ע����Ҵ���
        				,''																																			AS MSMBFlag								--���Ų�΢С��ҵ��ʶ

    FROM				RWA_DEV.FNS_BND_INFO_B T1
		INNER JOIN	TEMP_BND_BOOK T2
		ON 					T1.BOND_ID = T2.BOND_ID
		LEFT JOIN		RWA.ORG_INFO T3																            --RWA������
		ON					T1.DEPARTMENT = T3.ORGID
    WHERE 			(T1.ASSET_CLASS = '20' OR
								 (T1.ASSET_CLASS = '40' AND T1.BOND_TYPE1 NOT IN ('091','099')) OR
								 (T1.ASSET_CLASS = '40' AND T1.BOND_TYPE1 IN ('091','099') AND T1.CLOSED = '1')
								)
	  																																--ͨ���ʲ�������ȷ��ծȯ����Ӧ�տ�Ͷ�ʡ�
	  																																--40 �ɹ��������ʲ�
		--����Ϲ����ݼ���ծȯͶ�ʷ�Χ����Ҫ�ų���ծȯ���ࡱ=���ʲ�֧��֤ȯ���ļ�¼����Ϊ��һ�൥��ͨ����¼ģ���ȡ���ݡ���Ҫ������
		AND 				T1.BOND_TYPE1 <> '060'
		AND					T1.BOND_ID NOT IN
								(SELECT ZQNM FROM RWA.RWA_WSIB_ABS_ISSUE_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
								 UNION ALL
								 SELECT ZQNM FROM RWA.RWA_WSIB_ABS_INVEST_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
								)
																																		--�����ñ��ų��ʲ�֤ȯ����ծȯ����
		AND					T1.BOND_ID = 'B200801010095'												--ë��ϯ����
		AND 				T1.DATANO = p_data_dt_str														--ծȯ��Ϣ��,��ȡ��Ч��ծȯ��Ϣ
	  ;

		COMMIT;

		--���һ���ѺƷ������
    INSERT INTO RWA_DEV.RWA_TZ_CLIENT(
         DataDate                   --��������
        ,DataNo                     --������ˮ��
        ,ClientID                   --��������ID
        ,SourceClientID             --Դ��������ID
        ,SSysID                     --ԴϵͳID
        ,ClientName                 --������������
        ,SOrgID                     --Դ����ID
        ,SOrgName                   --Դ��������
        ,OrgSortNo                  --�������������
        ,OrgID                      --��������ID
        ,OrgName                    --������������
        ,IndustryID                 --������ҵ����
        ,IndustryName               --������ҵ����
        ,ClientType                 --�����������
        ,ClientSubType              --��������С��
        ,RegistState                --ע����һ����
        ,RCERating                  --����ע����ⲿ����
        ,RCERAgency                 --����ע����ⲿ��������
        ,OrganizationCode           --��֯��������
        ,ConsolidatedSCFlag         --�Ƿ񲢱��ӹ�˾
        ,SLClientFlag               --רҵ����ͻ���ʶ
        ,SLClientType               --רҵ����ͻ�����
        ,ExpoCategoryIRB            --��������¶���
        ,ModelID                    --ģ��ID
        ,ModelIRating               --ģ���ڲ�����
        ,ModelPD                    --ģ��ΥԼ����
        ,IRating                    --�ڲ�����
        ,PD                         --ΥԼ����
        ,DefaultFlag                --ΥԼ��ʶ
        ,NewDefaultFlag             --����ΥԼ��ʶ
        ,DefaultDate                --ΥԼʱ��
        ,ClientERating              --���������ⲿ����
        ,CCPFlag                    --���뽻�׶��ֱ�ʶ
        ,QualCCPFlag                --�Ƿ�ϸ����뽻�׶���
        ,ClearMemberFlag            --�����Ա��ʶ
        ,CompanySize                --��ҵ��ģ
        ,SSMBFlag                   --��׼С΢��ҵ��ʶ
        ,SSMBFLAGSTD         				--Ȩ�ط���׼С΢��ҵ��ʶ
        ,AnnualSale                 --��˾�ͻ������۶�
        ,CountryCode                --ע����Ҵ���
        ,MSMBFlag										--���Ų�΢С��ҵ��ʶ
    )
		SELECT 			--DISTINCT
        				 TO_DATE(p_data_dt_str,'yyyyMMdd')     																	AS DataDate            		--��������
        				,p_data_dt_str                         																	AS DataNo              		--������ˮ��
        				,T1.CUSTID1																															AS ClientID            		--��������ID
        				,T1.CUSTID1																															AS SourceClientID      		--Դ��������ID
        				,'TZ'                                 																	AS SSysID              		--ԴϵͳID
        				,T1.GUARANTORNAME																												AS ClientName          		--������������
        				,T1.BELONGORGCODE				              																	AS SOrgID              		--Դ����ID
        				,T3.ORGNAME																															AS SOrgName            		--Դ��������
        				,T3.SORTNO																															AS OrgSortNo           		--�������������
        				,T1.BELONGORGCODE					             																	AS OrgID               		--��������ID
        				,T3.ORGNAME																															AS OrgName             		--������������
        				,'J66'															  																	AS IndustryID          		--������ҵ����          Ĭ�� J66-���ҽ��ڷ���
        				,'���ҽ��ڷ���'							          																	AS IndustryName        		--������ҵ����          Ĭ�� J66-���ҽ��ڷ���
        				,SUBSTR(T1.GUARANTORCATEGORY,1,2)																				AS ClientType          		--�����������
        				,T1.GUARANTORCATEGORY																										AS ClientSubType       		--��������С��
        				,CASE WHEN T1.GUARANTORCOUNTRYCODE = 'CHN' THEN '01'
                 ELSE '02'
                 END	                                 																	AS RegistState         		--ע����һ����
        				,T5.RATINGRESULT                       																	AS RCERating           		--����ע����ⲿ����
        				,'01'                                  																	AS RCERAgency          		--����ע����ⲿ��������
        				,''																     																	AS OrganizationCode    		--��֯��������
        				,'0'	                                																	AS ConsolidatedSCFlag  		--�Ƿ񲢱��ӹ�˾
        				,'0'                                  																	AS SLClientFlag        		--רҵ����ͻ���ʶ
        				,''	                                  																	AS SLClientType        		--רҵ����ͻ�����
        				,''			                              																	AS ExpoCategoryIRB     		--��������¶���
        				,''				                            																	AS ModelID             		--ģ��ID
        				,''				                            																	AS ModelIRating        		--ģ���ڲ�����
        				,NULL                                 																	AS ModelPD             		--ģ��ΥԼ����
        				,''								                    																	AS IRating             		--�ڲ�����
        				,NULL	                                																	AS PD                  		--ΥԼ����
        				,'0'																																		AS DefaultFlag         		--ΥԼ��ʶ
        				,'0'											            																	AS NewDefaultFlag      		--����ΥԼ��ʶ
        				,''																     																	AS DefaultDate         		--ΥԼʱ��
        				,''																     																	AS ClientERating       		--���������ⲿ����
        				,'0'                                  																	AS CCPFlag             		--���뽻�׶��ֱ�ʶ
        				,'0'                                   																	AS QualCCPFlag         		--�Ƿ�ϸ����뽻�׶���
        				,'0'                                   																	AS ClearMemberFlag     		--�����Ա��ʶ
        				,'00'									                																	AS CompanySize         		--��ҵ��ģ
        				,'0'											            																	AS SSMBFlag            		--��׼С΢��ҵ��ʶ
        				,'0'											            																	AS SSMBFlagSTD         		--Ȩ�ط���׼С΢��ҵ��ʶ
        				,NULL							                     																	AS AnnualSale          		--��˾�ͻ������۶�
        				,'CHN'																																	AS CountryCode            --ע����Ҵ���
        				,''																																			AS MSMBFlag								--���Ų�΢С��ҵ��ʶ

    FROM				RWA.RWA_WS_BONDTRADE_MF T1	 																--���һ���ծȯͶ�ʲ�¼��
		INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT T2
    ON          T1.SUPPORGID = T2.ORGID
    AND         T2.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND         T2.SUPPTMPLID = 'M-0071'
    AND         T2.SUBMITFLAG = '1'
    LEFT JOIN		RWA.ORG_INFO T3
    ON					T1.BELONGORGCODE = T3.ORGID
    LEFT JOIN		RWA.RWA_WP_COUNTRYRATING T5
    ON 					T1.GUARANTORCOUNTRYCODE = T5.COUNTRYCODE
    AND					T5.ISINUSE = '1'
		WHERE 			T1.CUSTID1 IS NOT NULL
		AND					T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
	  ;

		COMMIT;

		dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TZ_CLIENT',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_TZ_CLIENT;
    --DBMS_OUTPUT.PUT_LINE('RWA_DEV.RWA_TZ_CLIENT���в�������Ϊ��' || v_count || '��');
    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�' || '-' || v_count;

    commit;
    --�����쳣
    EXCEPTION WHEN OTHERS THEN
    		--DBMS_OUTPUT.PUT_LINE('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '����ʹ�ϵͳ-��������('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm;
    		RETURN;

END PRO_RWA_TZ_CLIENT_0808;
/

