SLACK_MESSAGE_TITLE = "Health check"
SLACK_CHANNEL = "U018GESMPDJ"

CUSTOM_TICKET_FIELDS = {
    "internal": {
      "country": "cf_rand523064",
      "type_client": "cf_rand541330",
      "type_category": "cf_rand392121",
      "type_request": "cf_rand657438"
    },
    "external": {
      "country": "cf_rand749641",
      "type_client": "cf_rand938752",
      "type_category": "cf_rand987839",
      "type_request": "cf_rand444886"
    }
  }

DEFAULT_TICKET_FILED_IDS = {
    "internal": {
      "status": 48000329989,
      "agent": 48000329992,
      "group": 48000329991,
      "channel": 48000329988,
      "priority": 48000329990,
      "source_type": 48000838919,
      "client_type": 48000862826,
      "ticket_type": 48000329987
    },
    "external": {
      "status": 60000002116,
      "agent": 60000002119,
      "group": 60000002118,
      "channel": 60000002115,
      "priority": 60000002117,
      "client_type": 60000573937,
      "ticket_type": 60000002114
    }
  }

TICKET_TABLE_COLS = {
    "internal": [
      "id", "agent_id", "group_id", "status_id", "channel_id", "priority_id", "tags", "created_at", "resolved_at",
      "first_reply_at", "val_result", "source_type", "vvt_flags", "country", "id_project", "domain_project",
      "id_user_auction", "login_auction", "id_employer", "requester_id", "type_client", "type_category", "type_request",
      "ticket_type"
    ],
    "external": [
      "id", "agent_id", "group_id", "status_id", "channel_id", "priority_id", "tags", "created_at", "resolved_at",
      "first_reply_at", "country", "type_client", "type_category", "type_request", "ticket_type", "employer_project_id",
      "session_installation_id", "job_complaint_action_id"
    ]
  }

PARENT_TABLE_FIELDS = {
    "internal": [
      "agents", "groups", "statuses", "channels", "priorities", "client_types", "category_types", "request_types",
      "ticket_types", "requesters", "vvt_flags"
    ],
    "external": [
      "agents", "groups", "statuses", "channels", "priorities", "client_types", "category_types", "request_types",
      "ticket_types", "job_complaint_actions"
    ]
  }

TABLES_W_FK = {
    "internal": ["replies", "status_changes", "assignment_changes", "csat"],
    "external": ["replies", "status_changes", "assignment_changes", "csat_ru", "csat_en", "satisfaction_surveys"]
  }
