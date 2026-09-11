# Databricks notebook source
# MAGIC %md
# MAGIC ## 05. Reference & player table registry
# MAGIC Backs the generic parameterized ingestion notebook
# MAGIC (`02-bronze/07.Ingest Reference Table (Generic)`) and the bronze
# MAGIC validation notebook. One entry per small reference/master-data table --
# MAGIC these 14 tables all share the same shape (full overwrite,
# MAGIC JDBC, explicit column pinning), so one generic notebook ingests all of
# MAGIC them via a Databricks Job for-each task instead of 14
# MAGIC near-identical copy-pasted notebooks.
# MAGIC
# MAGIC To add another reference table later: add one entry here and one key to
# MAGIC the job's for-each list -- no new notebook needed.
# MAGIC
# MAGIC Generated from the same approved table design as the Step 1 Bronze Design
# MAGIC doc, so this registry cannot drift out of sync with the reviewed schema.

# COMMAND ----------

REFERENCE_TABLES = {
    "ref_countries": {
        "source_table": "Countries_TTU",
        "target_table": "ref_countries",
        "columns": ["CountryId", "CountryCode", "CountryName", "ContinentId", "IsActive", "Flag", "CreatedDateTime", "LastUpdatedDateTime", "CreatedBy", "LastModifiedBy", "IsDeleted", "TelCode", "IndividualNamingConvention"],
    },
    "ref_continents": {
        "source_table": "Continents",
        "target_table": "ref_continents",
        "columns": ["ContinentId", "ContinentCode", "ContinentName", "Subcontinent", "IsActive", "CreatedDateTime", "LastUpdatedDateTime"],
    },
    "ref_age_categories": {
        "source_table": "Age_Categories",
        "target_table": "ref_age_categories",
        "columns": ["AgeCategoryCode", "AgeCategoryDescription", "MinAge_Inclusive", "MaxAge_Inclusive", "CategoryCode", "OrganizationCode"],
    },
    "ref_categories": {
        "source_table": "Categories",
        "target_table": "ref_categories",
        "columns": ["CategoryCode", "CategoryDescription", "OrganizationCode"],
    },
    "ref_ranking_categories": {
        "source_table": "RankingCategories",
        "target_table": "ref_ranking_categories",
        "columns": ["RankingCategoryId", "RankingCategoryCode", "OrganizationCode", "CategoryCode", "RankingCategoryDesc", "RankingOrder", "AgeCategoryCode"],
    },
    "ref_subevent_types": {
        "source_table": "SubEventTypes",
        "target_table": "ref_subevent_types",
        "columns": ["SubEventTypeId", "SubEventTypeName", "SubEventTypeDesc", "IsActive", "CreatedDateTime", "LastUpdatedDateTime", "CreatedBy", "LastModifiedBy", "IsDeleted", "Gender", "SubEventCode", "SubEventODFCode"],
    },
    "ref_subevents_codes": {
        "source_table": "SubeventsCodes",
        "target_table": "ref_subevents_codes",
        "columns": ["SubEventCode", "SubEvent", "OrganizationCode"],
    },
    "ref_subevents_codes_description": {
        "source_table": "SubeventsCodes_Description",
        "target_table": "ref_subevents_codes_description",
        "columns": ["SubEventCode", "Description", "AgeCategoryCode", "CategoryCode", "OrganizationCode"],
    },
    "ref_subevent_dependent_categories": {
        "source_table": "SubEventDependentCategories",
        "target_table": "ref_subevent_dependent_categories",
        "columns": ["SubEventDependentCategoryId", "SubEventCode", "SubEventDependentCategoryCode", "CategoryCode", "OrganizationCode"],
    },
    "ref_organization": {
        "source_table": "Organization",
        "target_table": "ref_organization",
        "columns": ["OrganizationCode", "OrganizationName"],
    },
    "ref_event_type_general": {
        "source_table": "EventTypeGeneral",
        "target_table": "ref_event_type_general",
        "columns": ["EventTypeGeneralCode", "CategoryCode", "Description"],
    },
    "ref_result_position": {
        "source_table": "ResultPosition",
        "target_table": "ref_result_position",
        "columns": ["ResultPositionId", "Position", "Phase", "PositionOrder", "PhaseType", "RoundNumber", "PositionValue", "RankingCategoryCode", "AgeCategoryCode", "OrganizationCode", "EventTypeID", "CategoryCode"],
    },
    "competitors": {
        "source_table": "Competitors",
        "target_table": "competitors",
        "columns": ["PlayerID", "PlayerName", "Age", "Gender", "CountryCode", "DOB", "AgeCategoryCode", "FirstName", "Surname", "InsertUpdateDeleteFlag", "InsertDateTime", "InsertedBy", "UpdateDateTime", "UpdatedBy", "DeleteDateTime", "DeletedBy", "IsRetired", "OlympicEligibility", "TeamEligibility", "WorldTitleEligibility", "OrganizationCode", "NationalityCode"],
    },
    "players_doubles": {
        "source_table": "Players_Doubles",
        "target_table": "players_doubles",
        "columns": ["DoublesId", "Player1Id", "Player2Id", "SubEventCode", "AgeCategoryCode", "InsertUpdateDeleteFlag", "InsertDateTime", "InsertedBy", "UpdateDateTime", "UpdatedBy", "DeleteDateTime", "DeletedBy"],
    },
}