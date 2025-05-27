from typing import Dict, Any, Optional, Tuple, List
import pandas as pd
import json


def get_ip_reputation_info(ip_reputations: list) -> Tuple[Optional[str], Optional[List[Dict]]]:
    """
    Calculate weighted reputation score and collect problematic IPs.

    This function processes IP reputation data from Google Postmaster Tools API.
    It calculates a weighted average reputation score based on IP counts and their
    respective reputation levels, and collects information about problematic IPs
    (those with LOW or BAD reputation).

    Args:
        ip_reputations (list): List of IP reputation data from Postmaster API.
            Each item should contain 'ipCount', 'reputation', and optionally 'sampleIps'.

    Returns:
        Tuple[Optional[str], Optional[List[Dict]]]: A tuple containing:
            - Calculated reputation category (HIGH/MEDIUM/LOW/BAD) or None
            - List of problematic IP data (for LOW and BAD categories) or None

    Example of return value:
        ('MEDIUM', [
            {'category': 'LOW', 'ips': ['1.2.3.4', '5.6.7.8']},
            {'category': 'BAD', 'ips': ['9.10.11.12']}
        ])
    """
    if not ip_reputations:
        return None, None

    # Define reputation weights for score calculation
    REPUTATION_WEIGHTS = {
        'HIGH': 4,
        'MEDIUM': 3,
        'LOW': 2,
        'BAD': 1
    }

    total_ips = 0
    weighted_sum = 0
    problematic = []

    for rep in ip_reputations:
        ip_count = int(rep.get('ipCount', 0))
        reputation = rep.get('reputation')

        # Skip entries without ipCount
        if not ip_count:
            continue

        if reputation in REPUTATION_WEIGHTS:
            total_ips += ip_count
            weighted_sum += ip_count * REPUTATION_WEIGHTS[reputation]

            # Collect problematic IPs (LOW and BAD categories)
            if reputation in ['LOW', 'BAD']:
                problematic.append({
                    'category': reputation,
                    'ips': rep.get('sampleIps', [])
                })

    # Return None if no valid IP counts found
    if total_ips == 0:
        return None, None

    # Calculate average score
    avg_score = weighted_sum / total_ips

    # Convert score back to category using match-case
    match avg_score:
        case avg_score if avg_score >= 3.5:
            reputation = 'HIGH'
        case avg_score if avg_score >= 2.5:
            reputation = 'MEDIUM'
        case avg_score if avg_score >= 1.5:
            reputation = 'LOW'
        case _:
            reputation = 'BAD'

    return reputation, problematic if problematic else None


def process_postmaster_data(
    context,
    stats: Dict[str, Dict[str, Any]],
    date: str,
    map_country_code_to_id: dict
) -> pd.DataFrame:
    """
    Process postmaster stats data and return as DataFrame with optimized transformations.

    This function processes raw data from Google Postmaster Tools API into a structured
    DataFrame. It handles various data transformations including reputation calculations,
    ratio formatting, and JSON conversions for complex nested data.

    Args:
        context: Dagster context for logging
        stats (Dict[str, Dict[str, Any]]): Raw stats data from Postmaster API
        date (str): Date in YYYYMMDD format
        map_country_code_to_id (dict): Mapping of country codes to country IDs

    Returns:
        pd.DataFrame: Processed DataFrame with standardized columns and formats

    Raises:
        Exception: If there's an error during data processing
    """
    rows = []
    for domain, stats_data in stats.items():
        try:
            context.log.debug(f"Processing domain: {domain}")

            # Pre-process complex fields before adding to rows
            spammy_loops = stats_data.get('spammyFeedbackLoops', [])
            if spammy_loops:
                try:
                    transformed_loops = [
                        {
                            'id': loop.get('id'),
                            'spamRatio': round(loop.get('spamRatio', 0) * 100, 1)
                        }
                        for loop in spammy_loops
                    ]
                    spammy_loops_json = json.dumps(transformed_loops)
                except Exception as e:
                    context.log.error(f"Error processing spammy_feedback_loops for {domain}: {e}")
                    spammy_loops_json = None
            else:
                spammy_loops_json = None

            # Calculate IP reputation and get problematic IPs
            ip_reputations = stats_data.get('ipReputations', [])
            ip_reputation, problematic_ips = get_ip_reputation_info(ip_reputations)

            # Pre-process problematic IPs
            if problematic_ips:
                try:
                    transformed_ips = [
                        {
                            'category': item['category'].lower(),
                            'ips': item['ips']
                        }
                        for item in problematic_ips
                    ]
                    problematic_ips_json = json.dumps(transformed_ips)
                except Exception as e:
                    context.log.error(f"Error processing problematic_ips for {domain}: {e}")
                    problematic_ips_json = None
            else:
                problematic_ips_json = None

            # Pre-process delivery errors
            delivery_errors = stats_data.get('deliveryErrors', [])
            if delivery_errors:
                try:
                    transformed_errors = [
                        {
                            "errorClass": error.get("errorClass", "").lower(),
                            "errorType": error.get("errorType", "").lower(),
                            "errorRatio": round(error.get("errorRatio", 0) * 100, 1)
                        }
                        for error in delivery_errors
                        if "errorRatio" in error and error.get("errorRatio") is not None
                    ]
                    delivery_errors_json = json.dumps(transformed_errors) if transformed_errors else None
                except Exception as e:
                    context.log.error(f"Error processing delivery_errors for {domain}: {e}")
                    delivery_errors_json = None
            else:
                delivery_errors_json = None

            # Extract country code and ID from domain
            country_code = domain.split('.')[0].lower()
            if domain == 'jooble.org':
                country_code = 'us'
            country_id = map_country_code_to_id.get(country_code)

            # Construct row with pre-processed data
            row = {
                'domain': domain,
                'country_code': country_code,
                'country_id': country_id,
                'date': date,
                'user_reported_spam_ratio': stats_data.get('userReportedSpamRatio', 0),
                'spammy_feedback_loops': spammy_loops_json,
                'domain_reputation': stats_data.get('domainReputation'),
                'ip_reputation': ip_reputation,
                'problematic_ips': problematic_ips_json,
                'delivery_errors': delivery_errors_json,
                'spf_success_ratio': stats_data.get('spfSuccessRatio'),
                'dkim_success_ratio': stats_data.get('dkimSuccessRatio'),
                'dmarc_success_ratio': stats_data.get('dmarcSuccessRatio'),
            }
            rows.append(row)
        except Exception as e:
            context.log.error(f"Error processing domain {domain}: {e}")
            raise

    # Create DataFrame
    context.log.debug("Creating DataFrame...")
    df = pd.DataFrame(rows)

    try:
        # Format date for all rows at once
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')

        # Convert reputation columns to lowercase
        reputation_cols = ['domain_reputation', 'ip_reputation']
        for col in reputation_cols:
            df[col] = df[col].apply(lambda x: x.lower() if pd.notna(x) else x)

        # Format ratio columns (convert to percentages)
        ratio_cols = [
            'user_reported_spam_ratio',
            'spf_success_ratio',
            'dkim_success_ratio',
            'dmarc_success_ratio'
        ]
        for col in ratio_cols:
            df[col] = df[col].apply(lambda x: round(x * 100, 1) if pd.notna(x) else x)

    except Exception as e:
        context.log.error(f"Error during DataFrame transformation: {e}")
        context.log.error(f"DataFrame head at error: {df.head()}")
        context.log.error(f"DataFrame info: {df.info()}")
        raise

    return df
