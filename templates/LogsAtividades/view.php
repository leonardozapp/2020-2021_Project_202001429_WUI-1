<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\LogsAtividade $logsAtividade
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Html->link(__('Edit Logs Atividade'), ['action' => 'edit', $logsAtividade->id], ['class' => 'side-nav-item']) ?>
            <?= $this->Form->postLink(__('Delete Logs Atividade'), ['action' => 'delete', $logsAtividade->id], ['confirm' => __('Are you sure you want to delete # {0}?', $logsAtividade->id), 'class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('List Logs Atividades'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('New Logs Atividade'), ['action' => 'add'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="logsAtividades view content">
            <h3><?= h($logsAtividade->id) ?></h3>
            <table>
                <tr>
                    <th><?= __('Action') ?></th>
                    <td><?= h($logsAtividade->action) ?></td>
                </tr>
                <tr>
                    <th><?= __('RemoteURI') ?></th>
                    <td><?= h($logsAtividade->remoteURI) ?></td>
                </tr>
                <tr>
                    <th><?= __('LocalURI') ?></th>
                    <td><?= h($logsAtividade->localURI) ?></td>
                </tr>
                <tr>
                    <th><?= __('Message') ?></th>
                    <td><?= h($logsAtividade->message) ?></td>
                </tr>
                <tr>
                    <th><?= __('Id') ?></th>
                    <td><?= $this->Number->format($logsAtividade->id) ?></td>
                </tr>
                <tr>
                    <th><?= __('Result') ?></th>
                    <td><?= $this->Number->format($logsAtividade->result) ?></td>
                </tr>
                <tr>
                    <th><?= __('Date') ?></th>
                    <td><?= h($logsAtividade->date) ?></td>
                </tr>
                <tr>
                    <th><?= __('HasErrors') ?></th>
                    <td><?= $logsAtividade->hasErrors ? __('Yes') : __('No'); ?></td>
                </tr>
                <tr>
                    <th><?= __('NeedToRedo') ?></th>
                    <td><?= $logsAtividade->needToRedo ? __('Yes') : __('No'); ?></td>
                </tr>
            </table>
        </div>
    </div>
</div>
