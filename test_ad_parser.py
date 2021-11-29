import sys
sys.path.append("/home/runner/work/ads/ads") # required by Github action to import modules
from unittest.mock import patch, MagicMock
from test_helper import CURRENT_TIME
import pytest
import ad_parser.ad_parser
from ad_parser.ad_parser import ad_parser

@pytest.fixture
def mock_sites(monkeypatch):
    sites = {
                "abc.com": {
                    "ad_parser": MagicMock()
                }
            }
    monkeypatch.setattr("ad_parser.ad_parser.SITES", sites)
    return sites


@patch("ad_parser.ad_parser.time.time")
@patch("ad_parser.ad_parser.get_html")
@patch("ad_parser.ad_parser.add_to_queue")
# pylint: disable=too-many-arguments
# pylint: disable=redefined-outer-name
def test_ad_listing_parser(mock_add_to_queue,\
                           mock_get_html,\
                           mock_time,\
                           mock_sites):
    # pylint: enable=too-many-arguments
    # pylint: enable=redefined-outer-name
    mock_time.return_value = CURRENT_TIME
    domain = "abc.com"
    ad_listing_url = "abc.com/escorts"
    ad_url = "abc.com/escorts/1"
    message = {
	    "domain": domain,
	    "ad_listing_url": ad_listing_url,
	    "timestamps": {},
	    "ad_url": ad_url,
	    "ad_listing_data": {},
        "stored_ad_url": "abc.com/escorts/1"
    }

    mock_parser = mock_sites[message["domain"]]["ad_parser"]
    ad_parser_object  = {
        "primary_phone_number": "primary_number",
        "phone_numbers": "phone_numbers",
        "date_posted": "date_posted",
        "name": "name",
        "primary_email": "primary_email",
        "emails": "emails",
        "social": "social",
        "age": "age",
        "image_urls": "image_urls",
        "location": "location",
        "ethnicity": "ethnicity",
        "gender": "gender",
        "services": "services",
        "website": "website",
        "ad_text": "ad_text",
        "ad_title": "ad_title",
        "orientation": "orientation",
    }
    mock_parser().ad_dict.return_value = ad_parser_object
    mock_get_html.return_value = "<></>"
    new_message = {
        **message,
    	"ad_data": ad_parser_object,
    	"timestamps": {
    		"ad_parser": int(CURRENT_TIME)
    	}
	}

    ad_parser(message)
    mock_get_html.assert_called_with(domain, "abc.com/escorts/1")
    mock_parser.assert_called_with(mock_get_html())
    mock_parser.return_value.ad_dict.assert_called()
    mock_add_to_queue.assert_called_with("ad_processor", new_message)
