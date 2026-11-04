"""A bank deleted while its background work runs, then recreated under the same id, starts empty.

The report: delete a bank mid-consolidation or mid-refresh, create it again straight away, and the
"new" bank shows the deleted bank's data and timestamps. Each story parks the job inside its LLM
call (`llm.hold`), deletes and recreates the bank, then lets the job finish.
"""

import pytest
from hindsight_system_tests import reflect_loop
from hindsight_system_tests.payloads import consolidation, extracted, fact, observes

pytestmark = pytest.mark.asyncio


async def _assert_empty(client, bank_id: str) -> None:
    memories = (await client.memory.list_memories(bank_id, limit=100)).items
    assert [(m.fact_type, m.text) for m in memories] == []
    models = await client.alist_mental_models(bank_id)
    assert [m.name for m in models.items] == []
    # The stats and the profile are cached; neither may answer for the deleted bank.
    stats = await client.banks.get_agent_stats(bank_id)
    assert (stats.total_nodes, stats.total_documents, stats.last_consolidated_at) == (0, 0, None)
    banks = (await client.banks.list_banks()).banks
    assert [b.name for b in banks if b.bank_id == bank_id] == ["fresh"]


async def test_delete_and_recreate_during_consolidation(client, llm, bank_id, settled):
    llm.on_step("extract_facts").returns(extracted(fact("Alice moved to Berlin", who="Alice")))
    llm.on_step("consolidate").answers_with(observes("Alice lives in Berlin"))

    async with llm.hold("consolidate") as held:
        await client.aretain(bank_id=bank_id, content="Alice moved to Berlin.")
        await held.reached()
        await client.banks.delete_bank(bank_id)
        await client.acreate_bank(bank_id=bank_id, name="fresh")

    await settled(bank_id)
    await _assert_empty(client, bank_id)


async def test_delete_and_recreate_during_mental_model_refresh(client, llm, bank_id, settled):
    llm.on_step("extract_facts").returns(extracted(fact("Alice moved to Berlin", who="Alice")))
    llm.on_step("consolidate").returns(consolidation())
    reflect_loop(llm, answer="Alice lives in Berlin.")
    await client.aretain(bank_id=bank_id, content="Alice moved to Berlin.")
    await settled(bank_id)
    model = await client.mental_models.create_mental_model(
        bank_id, {"name": "Housing", "source_query": "Where does Alice live?"}
    )
    await settled(bank_id)

    async with llm.hold("reflect") as held:
        await client.mental_models.refresh_mental_model(bank_id, model.mental_model_id)
        await held.reached()
        await client.banks.delete_bank(bank_id)
        await client.acreate_bank(bank_id=bank_id, name="fresh")

    await settled(bank_id)
    await _assert_empty(client, bank_id)
