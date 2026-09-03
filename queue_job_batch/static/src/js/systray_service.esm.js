/** @odoo-module **/

import { Component, useState, onMounted, onWillUnmount, useRef } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { registry } from "@web/core/registry";

class QueueJobBatchMenu extends Component {
    static template = "queue_job_batch.QueueJobBatchMenuView";

    setup() {
        this.orm = useService("orm");
        this.action = useService("action");
        this.user = useService("user");
        this.rootRef = useRef("root");
        this.state = useState({ isOpen: false, batches: [] });

        this._onClickCaptureGlobal = this._onClickCaptureGlobal.bind(this);

        onMounted(() => {
            this.fetchData();
            document.addEventListener("click", this._onClickCaptureGlobal, true);
        });

        onWillUnmount(() => {
            document.removeEventListener("click", this._onClickCaptureGlobal, true);
        });
    }

    async fetchData() {
        this.state.batches = await this.orm.searchRead(
            "queue.job.batch",
            [
                ["user_id", "=", this.user.userId],
                "|",
                ["state", "in", ["draft", "progress"]],
                ["is_read", "=", false],
            ],
            [
                "name",
                "job_count",
                "completeness",
                "failed_percentage",
                "finished_job_count",
                "failed_job_count",
                "state",
            ]
        );
    }

    get counter() {
        return this.state.batches.length;
    }

    onClickDropdownToggle(ev) {
        ev.preventDefault();
        this.state.isOpen = !this.state.isOpen;
        if (this.state.isOpen) {
            this.fetchData();
        }
    }

    async onClickBatch(batch) {
        this.state.isOpen = false;
        this.state.batches = this.state.batches.filter((b) => b.id !== batch.id);
        await this.orm.call("queue.job.batch", "set_read", [batch.id]);
        this.action.doAction({
            type: "ir.actions.act_window",
            name: "Job batches",
            res_model: "queue.job.batch",
            views: [[false, "form"]],
            res_id: batch.id,
        });
    }

    async onHideBatch(batch) {
        this.state.batches = this.state.batches.filter((b) => b.id !== batch.id);
        await this.orm.call("queue.job.batch", "set_read", [batch.id]);
    }

    viewAll() {
        this.state.isOpen = false;
        this.action.doAction("queue_job_batch.action_view_your_queue_job_batch");
    }

    _onClickCaptureGlobal(ev) {
        const root = this.rootRef.el;
        if (root && root.contains(ev.target)) {
            return;
        }
        this.state.isOpen = false;
    }
}

const systrayService = {
    dependencies: ["user"],
    start(_env, { user }) {
        if (user.hasGroup("queue_job_batch.group_queue_job_batch_user")) {
            registry
                .category("systray")
                .add(
                    "queue_job_batch.QueueJobBatchMenu",
                    { Component: QueueJobBatchMenu },
                    { sequence: 99 }
                );
        }
    },
};

registry.category("services").add("queuebatch_systray_service", systrayService);
