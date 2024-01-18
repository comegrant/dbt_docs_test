select
    pr.product_id,
    pr.preference_id,
    MAX(lower_rule.rule_value) lower_rule_value,
    MAX(upper_rule.rule_value) upper_rule_value
    from cms.preference_rule pr
    left outer join cms.preference_rule lower_rule ON pr.id = lower_rule.id AND lower_rule.preference_rule_type_id = '8e20dc8a-cbed-41fe-ae0c-07d103c73df8'
    left outer join cms.preference_rule upper_rule ON pr.id = upper_rule.id AND upper_rule.preference_rule_type_id = '1d66094e-139e-471e-89b9-35a71a019673'
    group by pr.product_id, pr.preference_id
