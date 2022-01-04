<?php
declare(strict_types=1);

namespace App\Model\Entity;

use Cake\ORM\Entity;

/**
 * CarteirasInvestimento Entity
 *
 * @property int $id
 * @property int $usuario_id
 * @property string $nome
 * @property string $descricao
 *
 * @property \App\Model\Entity\Usuario $usuario
 * @property \App\Model\Entity\IndicadoresCarteira[] $indicadores_carteiras
 * @property \App\Model\Entity\OperacoesFinanceira[] $operacoes_financeiras
 */
class CarteirasInvestimento extends Entity
{
    /**
     * Fields that can be mass assigned using newEntity() or patchEntity().
     *
     * Note that when '*' is set to true, this allows all unspecified fields to
     * be mass assigned. For security purposes, it is advised to set '*' to false
     * (or remove it), and explicitly make individual fields accessible as needed.
     *
     * @var array
     */
    protected $_accessible = [
        'usuario_id' => true,
        'nome' => true,
        'descricao' => true,
        'usuario' => true,
        'indicadores_carteiras' => true,
        'operacoes_financeiras' => true,
    ];
}
