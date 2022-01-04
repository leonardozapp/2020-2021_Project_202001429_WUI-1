<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\RetornoRiscoFundo[]|\Cake\Collection\CollectionInterface $retornoRiscoFundos
 */
?>
<div class="retornoRiscoFundos index content">
    <?= $this->Html->link(__('New Retorno Risco Fundo'), ['action' => 'add'], ['class' => 'button float-right']) ?>
    <h3><?= __('Retorno Risco Fundos Table') ?></h3>
    <div class="table-responsive">
        <table>
            <thead>
                <tr>
                    <th><?= $this->Paginator->sort('cnpj_fundo_id', 'Fundo') ?></th>
                    <th><?= $this->Paginator->sort('periodo_meses') ?></th>
                    <th><?= $this->Paginator->sort('data_final') ?></th>
                    <th><?= $this->Paginator->sort('rentab_media') ?></th>
                    <th><?= $this->Paginator->sort('desvio_padrao') ?></th>
                    <th><?= $this->Paginator->sort('num_valores') ?></th>
                    <th><?= $this->Paginator->sort('rentab_min') ?></th>
                    <th><?= $this->Paginator->sort('rentab_max') ?></th>
                    <th><?= $this->Paginator->sort('meses_abaixo_bench') ?></th>
                    <th><?= $this->Paginator->sort('meses_acima_bench') ?></th>
                    <th class="actions"><?= __('Actions') ?></th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($retornoRiscoFundos as $retornoRiscoFundo): ?>
                <tr>
                    <td><?= $retornoRiscoFundo->has('cnpj_fundo') ? $this->Html->link($retornoRiscoFundo->cnpj_fundo->DENOM_SOCIAL, ['controller' => 'CnpjFundos', 'action' => 'view', $retornoRiscoFundo->cnpj_fundo->id]) : '' ?></td>
                    <td><?= $this->Number->format($retornoRiscoFundo->periodo_meses) ?></td>
                    <td><?= h($retornoRiscoFundo->data_final) ?></td>
                    <td><?= $this->Number->format($retornoRiscoFundo->rentab_media) ?></td>
                    <td><?= $this->Number->format($retornoRiscoFundo->desvio_padrao) ?></td>
                    <td><?= $this->Number->format($retornoRiscoFundo->num_valores) ?></td>
                    <td><?= $this->Number->format($retornoRiscoFundo->rentab_min) ?></td>
                    <td><?= $this->Number->format($retornoRiscoFundo->rentab_max) ?></td>
                    <td><?= $this->Number->format($retornoRiscoFundo->meses_abaixo_bench) ?></td>
                    <td><?= $this->Number->format($retornoRiscoFundo->meses_acima_bench) ?></td>
                    <td class="actions">
                        <?= $this->Html->link(__('View'), ['action' => 'view', $retornoRiscoFundo->cnpj_fundo_id]) ?>
                        <?= $this->Html->link(__('Edit'), ['action' => 'edit', $retornoRiscoFundo->cnpj_fundo_id]) ?>
                        <?= $this->Form->postLink(__('Delete'), ['action' => 'delete', $retornoRiscoFundo->cnpj_fundo_id], ['confirm' => __('Are you sure you want to delete # {0}?', $retornoRiscoFundo->cnpj_fundo_id)]) ?>
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
