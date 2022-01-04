<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\LogsAtividade[]|\Cake\Collection\CollectionInterface $logsAtividades
 */
?>
<div class="logsAtividades index content">
    <?= $this->Html->link(__('New Logs Atividade'), ['action' => 'add'], ['class' => 'button float-right']) ?>
    <h3><?= __('Logs Atividades') ?></h3>
    <div class="table-responsive">
        <table>
            <thead>
                <tr>
                    <th><?= $this->Paginator->sort('id') ?></th>
                    <th><?= $this->Paginator->sort('action') ?></th>
                    <th><?= $this->Paginator->sort('remoteURI') ?></th>
                    <th><?= $this->Paginator->sort('localURI') ?></th>
                    <th><?= $this->Paginator->sort('result') ?></th>
                    <th><?= $this->Paginator->sort('hasErrors') ?></th>
                    <th><?= $this->Paginator->sort('message') ?></th>
                    <th><?= $this->Paginator->sort('date') ?></th>
                    <th><?= $this->Paginator->sort('needToRedo') ?></th>
                    <th class="actions"><?= __('Actions') ?></th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($logsAtividades as $logsAtividade): ?>
                <tr>
                    <td><?= $this->Number->format($logsAtividade->id) ?></td>
                    <td><?= h($logsAtividade->action) ?></td>
                    <td><?= h($logsAtividade->remoteURI) ?></td>
                    <td><?= h($logsAtividade->localURI) ?></td>
                    <td><?= $this->Number->format($logsAtividade->result) ?></td>
                    <td><?= h($logsAtividade->hasErrors) ?></td>
                    <td><?= h($logsAtividade->message) ?></td>
                    <td><?= h($logsAtividade->date) ?></td>
                    <td><?= h($logsAtividade->needToRedo) ?></td>
                    <td class="actions">
                        <?= $this->Html->link(__('View'), ['action' => 'view', $logsAtividade->id]) ?>
                        <?= $this->Html->link(__('Edit'), ['action' => 'edit', $logsAtividade->id]) ?>
                        <?= $this->Form->postLink(__('Delete'), ['action' => 'delete', $logsAtividade->id], ['confirm' => __('Are you sure you want to delete # {0}?', $logsAtividade->id)]) ?>
                    </td>
                </tr>
                <?php endforeach; ?>
            </tbody>
        </table>
    </div>
    <div class="paginator">
        <ul class="pagination">
            <?= $this->Paginator->first('<< ' . __('first')) ?>
            <?= $this->Paginator->prev('< ' . __('previous')) ?>
            <?= $this->Paginator->numbers() ?>
            <?= $this->Paginator->next(__('next') . ' >') ?>
            <?= $this->Paginator->last(__('last') . ' >>') ?>
        </ul>
        <p><?= $this->Paginator->counter(__('Página {{page}} de {{pages}}, mostrando {{current}} registro(s) do total de {{count}}')) ?></p>
    </div>
</div>
