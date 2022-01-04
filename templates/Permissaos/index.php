<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\Permissao[]|\Cake\Collection\CollectionInterface $permissaos
 */
?>
<div class="permissaos index content">
    <?= $this->Html->link(__('New Permissao'), ['action' => 'add'], ['class' => 'button float-right']) ?>
    <h3><?= __('Permissaos') ?></h3>
    <div class="table-responsive">
        <table>
            <thead>
                <tr>
                    <th><?= $this->Paginator->sort('id') ?></th>
                    <th><?= $this->Paginator->sort('supor_fck_root') ?></th>
                    <th><?= $this->Paginator->sort('administrador_mng') ?></th>
                    <th><?= $this->Paginator->sort('carteiras_mng') ?></th>
                    <th><?= $this->Paginator->sort('logs_mng') ?></th>
                    <th><?= $this->Paginator->sort('operacoes_mng') ?></th>
                    <th><?= $this->Paginator->sort('usuarios_mng') ?></th>
                    <th><?= $this->Paginator->sort('tipos_mng') ?></th>
                    <th><?= $this->Paginator->sort('rel_mng') ?></th>
                    <th class="actions"><?= __('Actions') ?></th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($permissaos as $permissao): ?>
                <tr>
                    <td><?= $this->Number->format($permissao->id) ?></td>
                    <td><?= h($permissao->supor_fck_root) ?></td>
                    <td><?= h($permissao->administrador_mng) ?></td>
                    <td><?= h($permissao->carteiras_mng) ?></td>
                    <td><?= h($permissao->logs_mng) ?></td>
                    <td><?= h($permissao->operacoes_mng) ?></td>
                    <td><?= h($permissao->usuarios_mng) ?></td>
                    <td><?= h($permissao->tipos_mng) ?></td>
                    <td><?= h($permissao->rel_mng) ?></td>
                    <td class="actions">
                        <?= $this->Html->link(__('View'), ['action' => 'view', $permissao->id]) ?>
                        <?= $this->Html->link(__('Edit'), ['action' => 'edit', $permissao->id]) ?>
                        <?= $this->Form->postLink(__('Delete'), ['action' => 'delete', $permissao->id], ['confirm' => __('Are you sure you want to delete # {0}?', $permissao->id)]) ?>
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
        <p><?= $this->Paginator->counter(__('Page {{page}} of {{pages}}, showing {{current}} record(s) out of {{count}} total')) ?></p>
    </div>
</div>
