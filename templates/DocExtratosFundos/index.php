<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\DocExtratosFundo[]|\Cake\Collection\CollectionInterface $docExtratosFundos
 */
?>
<div class="docExtratosFundos index content">
    <?= $this->Html->link(__('New Doc Extratos Fundo'), ['action' => 'add'], ['class' => 'button float-right']) ?>
    <h3><?= __('Doc Extratos Fundos') ?></h3>
    <div class="table-responsive">
        <table>
            <thead>
                <tr>
                    <th><?= $this->Paginator->sort('cnpj_fundo_id') ?></th>
                    <th><?= $this->Paginator->sort('DT_COMPTC') ?></th>
                    <th><?= $this->Paginator->sort('tipo_anbima_classe_id') ?></th>
                    <th><?= $this->Paginator->sort('APLIC_MIN') ?></th>
                    <th><?= $this->Paginator->sort('QT_DIA_PAGTO_COTA') ?></th>
                    <th><?= $this->Paginator->sort('QT_DIA_PAGTO_RESGATE') ?></th>
                    <th><?= $this->Paginator->sort('PR_COTA_ETF_MAX') ?></th>
                    <th class="actions"><?= __('Actions') ?></th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($docExtratosFundos as $docExtratosFundo): ?>
                <tr>
                    <td><?= $docExtratosFundo->has('cnpj_fundo') ? $this->Html->link($docExtratosFundo->cnpj_fundo->id, ['controller' => 'CnpjFundos', 'action' => 'view', $docExtratosFundo->cnpj_fundo->id]) : '' ?></td>
                    <td><?= h($docExtratosFundo->DT_COMPTC->format('d/m/Y')) ?></td>
                    <td><?= $docExtratosFundo->has('tipo_anbima_class') ? $this->Html->link($docExtratosFundo->tipo_anbima_class->id, ['controller' => 'TipoAnbimaClasses', 'action' => 'view', $docExtratosFundo->tipo_anbima_class->id]) : '' ?></td>
                    <td><?= $this->Number->format($docExtratosFundo->APLIC_MIN) ?></td>
                    <td><?= $this->Number->format($docExtratosFundo->QT_DIA_PAGTO_COTA) ?></td>
                    <td><?= $this->Number->format($docExtratosFundo->QT_DIA_PAGTO_RESGATE) ?></td>
                    <td><?= $this->Number->format($docExtratosFundo->PR_COTA_ETF_MAX) ?></td>
                    <td class="actions">
                        <?= $this->Html->link(__('View'), ['action' => 'view', $docExtratosFundo->cnpj_fundo_id]) ?>
                        <?= $this->Html->link(__('Edit'), ['action' => 'edit', $docExtratosFundo->cnpj_fundo_id]) ?>
                        <?= $this->Form->postLink(__('Delete'), ['action' => 'delete', $docExtratosFundo->cnpj_fundo_id], ['confirm' => __('Are you sure you want to delete # {0}?', $docExtratosFundo->cnpj_fundo_id)]) ?>
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
