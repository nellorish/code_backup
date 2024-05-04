WITH
final as (
      SELECT
            TransNo as identifier,
            mmx_transaction.mmxkey as SourceIdentifier,
            CAST(ROW( 'http://hl7.org/fhir/ValueSet/payment-type', TransactionCode) as ROW("System" varchar, "Code" int)) as TypeConceptId,
            CAST(ROW( 'http://hl7.org/fhir/ValueSet/payment-kind', null) as ROW("System" varchar, "Code" int)) as KindConceptId,
            CAST(ROW( 'http://hl7.org/fhir/ValueSet/payment-issuertype', null) as ROW("System" varchar, "Code" int)) as IssuerTypeConceptId,
            CAST(ROW( 'http://terminology.hl7.org/CodeSystem/v2-0570', null) as ROW("System" varchar, "Code" int)) as MethodConceptID,
            cast(null as VARCHAR) as Status,
            cast(null as VARCHAR) as Deposition,
            cast(null as VARCHAR) as CardBrand,
            cast(null as varchar) as Outcome,
            cast(null as varchar) as disposition,
            cast(null as date) as ExpirationDate,
            cast(null as varchar) as Processor,
            cast(null as varchar) as PaymentAuthorization,
            cast(null as double precision ) as TenderedAmount,
            cast(null as double precision) as ReturnedAmount,
            TransactionDate as CreatedDate,
            mmx_transaction.postingDate as StartDate,
            Transactionamount  as Amount,
            mmx_transaction.TransactionCode as PaymentIdentifier,
    ARRAY[
          CAST(ROW( 'Account', mmx_transaction.accountnumber, 'ReferenceNumber') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar)),
          CAST(ROW( 'Organization', mmx_transaction.facilityidentifier, 'OrganizationPaymentIssuerID') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar)),
          CAST(ROW( 'Organization',mmx_transaction.facilityidentifier, 'OrganizationRequestorId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar)),
          CAST(ROW( 'Encounter',mmx_transaction.patientnumber, 'OrganizationRequestId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar))
        ] as ForeignKeys
     FROM {{source ('fhir4ingestion', 'mmx_transaction')}} mmx_transaction

)
select * from final