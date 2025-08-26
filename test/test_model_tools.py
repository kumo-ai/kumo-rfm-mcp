from unittest.mock import Mock, patch

import pytest

from kumo_rfm_mcp.tools.model_tools import register_model_tools


@pytest.fixture
def model_funcs():
    """Extract model tool functions."""
    mock_mcp = Mock()
    captured_funcs = []

    def capture_tool(func):
        captured_funcs.append(func)
        return Mock()

    mock_mcp.tool.side_effect = lambda: capture_tool
    register_model_tools(mock_mcp)

    return {
        'finalize_graph': captured_funcs[0],
        'validate_query': captured_funcs[1],
        'predict': captured_funcs[2],
        'evaluate': captured_funcs[3]
    }


@pytest.mark.asyncio
class TestFinalizeGraph:
    """Test the finalize_graph tool."""

    @patch('kumo_rfm_mcp.tools.model_tools.SessionManager.get_default_session')
    @patch('kumo_rfm_mcp.tools.model_tools.rfm.KumoRFM')
    async def test_finalize_graph_success(self, mock_kumo_rfm,
                                          mock_get_session, model_funcs):
        """Test successful graph finalization."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        mock_model = Mock()
        mock_kumo_rfm.return_value = mock_model

        result = await model_funcs['finalize_graph']()

        assert result['success'] is True
        assert "Successfully finalized graph" in result['message']
        mock_kumo_rfm.assert_called_once_with(mock_session.graph,
                                              verbose=False)
        assert mock_session.model == mock_model

    @patch('kumo_rfm_mcp.tools.model_tools.SessionManager.get_default_session')
    @patch('kumo_rfm_mcp.tools.model_tools.rfm.KumoRFM')
    async def test_finalize_graph_error(self, mock_kumo_rfm, mock_get_session,
                                        model_funcs):
        """Test graph finalization error handling."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        mock_kumo_rfm.side_effect = Exception("Graph validation failed")

        result = await model_funcs['finalize_graph']()

        assert result['success'] is False
        assert "Failed to finalize graph" in result['message']
        assert "Graph validation failed" in result['message']


@pytest.mark.asyncio
class TestValidateQuery:
    """Test the validate_query tool."""

    @patch('kumo_rfm_mcp.tools.model_tools.SessionManager.get_default_session')
    async def test_validate_query_success(self, mock_get_session, model_funcs):
        """Test successful query validation."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        mock_model = Mock()
        mock_session.model = mock_model

        query = "PREDICT COUNT(orders.*, 0, 30, days)>0 FOR users.user_id=1"

        result = await model_funcs['validate_query'](query)

        assert result['success'] is True
        assert "Query validated successfully" in result['message']
        mock_model._parse_query.assert_called_once_with(query)

    @patch('kumo_rfm_mcp.tools.model_tools.SessionManager.get_default_session')
    async def test_validate_query_no_model(self, mock_get_session,
                                           model_funcs):
        """Test query validation when no model is available."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        mock_session.model = None

        query = "PREDICT COUNT(orders.*, 0, 30, days)>0 FOR users.user_id=1"

        result = await model_funcs['validate_query'](query)

        assert result['success'] is False
        assert "No model available" in result['message']
        assert "finalize_graph" in result['message']

    @patch('kumo_rfm_mcp.tools.model_tools.SessionManager.get_default_session')
    async def test_validate_query_syntax_error(self, mock_get_session,
                                               model_funcs):
        """Test query validation with syntax errors."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        mock_model = Mock()
        mock_session.model = mock_model

        invalid_query = "INVALID QUERY SYNTAX"
        mock_model._parse_query.side_effect = Exception("Invalid PQL syntax")

        result = await model_funcs['validate_query'](invalid_query)

        assert result['success'] is False
        assert "Query validation failed" in result['message']
        assert "Invalid PQL syntax" in result['message']


@pytest.mark.asyncio
class TestPredict:
    """Test the predict tool."""

    @patch('kumo_rfm_mcp.tools.model_tools.SessionManager.get_default_session')
    async def test_predict_success(self, mock_get_session, model_funcs):
        """Test successful prediction."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        mock_model = Mock()
        mock_session.model = mock_model

        # Mock prediction result
        mock_result_df = Mock()
        mock_result_df.to_dict.return_value = [{
            "ENTITY": 1,
            "TARGET_PRED": True,
            "True_PROB": 0.85,
            "False_PROB": 0.15
        }]
        mock_model.predict.return_value = mock_result_df

        query = "PREDICT COUNT(orders.*, 0, 30, days)>0 FOR users.user_id=1"

        result = await model_funcs['predict'](query)

        assert result['success'] is True
        assert "Prediction completed successfully" in result['message']
        assert 'predictions' in result['data']
        assert len(result['data']['predictions']) == 1
        assert result['data']['predictions'][0]['ENTITY'] == 1
        mock_model.predict.assert_called_once_with(query, verbose=False)

    @patch('kumo_rfm_mcp.tools.model_tools.SessionManager.get_default_session')
    async def test_predict_no_model(self, mock_get_session, model_funcs):
        """Test prediction when no model is available."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        mock_session.model = None

        query = "PREDICT COUNT(orders.*, 0, 30, days)>0 FOR users.user_id=1"

        result = await model_funcs['predict'](query)

        assert result['success'] is False
        assert "No model available" in result['message']
        assert "finalize_graph" in result['message']

    @patch('kumo_rfm_mcp.tools.model_tools.SessionManager.get_default_session')
    async def test_predict_error(self, mock_get_session, model_funcs):
        """Test prediction error handling."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        mock_model = Mock()
        mock_session.model = mock_model

        query = "PREDICT COUNT(orders.*, 0, 30, days)>0 FOR users.user_id=1"
        mock_model.predict.side_effect = Exception("Prediction failed")

        result = await model_funcs['predict'](query)

        assert result['success'] is False
        assert "Prediction failed" in result['message']


@pytest.mark.asyncio
class TestEvaluate:
    """Test the evaluate tool."""

    @patch('kumo_rfm_mcp.tools.model_tools.SessionManager.get_default_session')
    async def test_evaluate_success(self, mock_get_session, model_funcs):
        """Test successful evaluation."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        mock_model = Mock()
        mock_session.model = mock_model

        # Mock evaluation result
        mock_result_df = Mock()
        mock_result_df.to_dict.return_value = [{
            "metric": "auroc",
            "value": 0.87
        }, {
            "metric": "auprc",
            "value": 0.56
        }]
        mock_model.evaluate.return_value = mock_result_df

        query = "PREDICT COUNT(orders.*, 0, 30, days)>0 FOR users.user_id=1"

        result = await model_funcs['evaluate'](query)

        assert result['success'] is True
        assert "Evaluation completed successfully" in result['message']
        assert 'metrics' in result['data']
        assert len(result['data']['metrics']) == 2
        assert result['data']['metrics'][0]['metric'] == 'auroc'
        assert result['data']['metrics'][0]['value'] == 0.87
        mock_model.evaluate.assert_called_once_with(query, verbose=False)

    @patch('kumo_rfm_mcp.tools.model_tools.SessionManager.get_default_session')
    async def test_evaluate_no_model(self, mock_get_session, model_funcs):
        """Test evaluation when no model is available."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        mock_session.model = None

        query = "PREDICT COUNT(orders.*, 0, 30, days)>0 FOR users.user_id=1"

        result = await model_funcs['evaluate'](query)

        assert result['success'] is False
        assert "No model available" in result['message']
        assert "finalize_graph" in result['message']

    @patch('kumo_rfm_mcp.tools.model_tools.SessionManager.get_default_session')
    async def test_evaluate_error(self, mock_get_session, model_funcs):
        """Test evaluation error handling."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        mock_model = Mock()
        mock_session.model = mock_model

        query = "PREDICT COUNT(orders.*, 0, 30, days)>0 FOR users.user_id=1"
        mock_model.evaluate.side_effect = Exception("Evaluation failed")

        result = await model_funcs['evaluate'](query)

        assert result['success'] is False
        assert "Evaluation failed" in result['message']


@pytest.mark.asyncio
class TestModelToolsEdgeCases:
    """Test edge cases for model tools."""

    @patch('kumo_rfm_mcp.tools.model_tools.SessionManager.get_default_session')
    async def test_finalize_graph_twice(self, mock_get_session, model_funcs):
        """Test finalizing graph multiple times (should work)."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        # First finalization
        with patch(
                'kumo_rfm_mcp.tools.model_tools.rfm.KumoRFM') as mock_kumo_rfm:
            mock_model1 = Mock()
            mock_kumo_rfm.return_value = mock_model1

            result1 = await model_funcs['finalize_graph']()
            assert result1['success'] is True
            assert mock_session.model == mock_model1

        # Second finalization (overwrites previous model)
        with patch(
                'kumo_rfm_mcp.tools.model_tools.rfm.KumoRFM') as mock_kumo_rfm:
            mock_model2 = Mock()
            mock_kumo_rfm.return_value = mock_model2

            result2 = await model_funcs['finalize_graph']()
            assert result2['success'] is True
            assert mock_session.model == mock_model2

    @patch('kumo_rfm_mcp.tools.model_tools.SessionManager.get_default_session')
    async def test_predict_empty_result(self, mock_get_session, model_funcs):
        """Test prediction with empty results."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        mock_model = Mock()
        mock_session.model = mock_model

        mock_result_df = Mock()
        mock_result_df.to_dict.return_value = []
        mock_model.predict.return_value = mock_result_df

        query = "PREDICT COUNT(orders.*, 0, 30, days)>0 FOR users.user_id=999"

        result = await model_funcs['predict'](query)

        assert result['success'] is True
        assert len(result['data']['predictions']) == 0

    @patch('kumo_rfm_mcp.tools.model_tools.SessionManager.get_default_session')
    async def test_evaluate_single_metric(self, mock_get_session, model_funcs):
        """Test evaluation with single metric result."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        mock_model = Mock()
        mock_session.model = mock_model

        mock_result_df = Mock()
        mock_result_df.to_dict.return_value = [{
            "metric": "accuracy",
            "value": 0.92
        }]
        mock_model.evaluate.return_value = mock_result_df

        query = "PREDICT COUNT(orders.*, 0, 30, days)>0 FOR users.user_id=1"

        result = await model_funcs['evaluate'](query)

        assert result['success'] is True
        assert len(result['data']['metrics']) == 1
        assert result['data']['metrics'][0]['metric'] == 'accuracy'
        assert result['data']['metrics'][0]['value'] == 0.92
